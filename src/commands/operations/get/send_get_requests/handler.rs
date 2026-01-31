use std::{sync::Arc, time::Instant};

use futures::{StreamExt, stream::FuturesUnordered};
use libp2p::PeerId;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::BlockchainManager,
        network::{NetworkError, NetworkManager, messages::GetRequestData},
        repository::RepositoryManager,
        triple_store::Assertion,
    },
    operations::{GetOperationResult, protocols},
    services::{
        GetValidationService, PeerPerformanceTracker, TripleStoreService,
        operation_status::OperationStatusService as GenericOperationService,
    },
    types::{ParsedUal, Visibility, parse_ual},
};

/// Command data for sending get requests to network nodes.
#[derive(Clone)]
pub(crate) struct SendGetRequestsCommandData {
    pub operation_id: Uuid,
    pub ual: String,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub visibility: Visibility,
}

impl SendGetRequestsCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        ual: String,
        include_metadata: bool,
        paranet_ual: Option<String>,
        visibility: Visibility,
    ) -> Self {
        Self {
            operation_id,
            ual,
            include_metadata,
            paranet_ual,
            visibility,
        }
    }
}

pub(crate) struct SendGetRequestsCommandHandler {
    pub(super) blockchain_manager: Arc<BlockchainManager>,
    pub(super) triple_store_service: Arc<TripleStoreService>,
    pub(super) repository_manager: Arc<RepositoryManager>,
    pub(super) network_manager: Arc<NetworkManager>,
    pub(super) get_operation_status_service: Arc<GenericOperationService<GetOperationResult>>,
    pub(super) get_validation_service: Arc<GetValidationService>,
    pub(super) peer_performance_tracker: Arc<PeerPerformanceTracker>,
}

impl SendGetRequestsCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            get_operation_status_service: Arc::clone(context.get_operation_status_service()),
            get_validation_service: Arc::clone(context.get_validation_service()),
            peer_performance_tracker: Arc::clone(context.peer_performance_tracker()),
        }
    }
}

impl CommandHandler<SendGetRequestsCommandData> for SendGetRequestsCommandHandler {
    async fn execute(&self, data: &SendGetRequestsCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;
        let ual = &data.ual;

        tracing::info!(
            operation_id = %operation_id,
            ual = %ual,
            include_metadata = data.include_metadata,
            visibility = ?data.visibility,
            "Starting SendGetRequests command"
        );

        // Parse the UAL
        let parsed_ual = match parse_ual(ual) {
            Ok(parsed) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    blockchain = %parsed.blockchain,
                    contract = ?parsed.contract,
                    kc_id = parsed.knowledge_collection_id,
                    ka_id = ?parsed.knowledge_asset_id,
                    "Parsed UAL"
                );
                parsed
            }
            Err(e) => {
                let error_message = format!("Invalid UAL format: {}", e);
                tracing::error!(operation_id = %operation_id, error = %e, "Failed to parse UAL");
                self.get_operation_status_service
                    .mark_failed(operation_id, error_message)
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

        // Validate UAL exists on-chain by checking if it has a publisher
        match self
            .blockchain_manager
            .get_knowledge_collection_publisher(
                &parsed_ual.blockchain,
                parsed_ual.contract,
                parsed_ual.knowledge_collection_id,
            )
            .await
        {
            Ok(Some(_publisher)) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    kc_id = parsed_ual.knowledge_collection_id,
                    "Knowledge collection validated on-chain"
                );
            }
            Ok(None) => {
                let error_message = format!(
                    "Knowledge collection {} does not exist on blockchain {}",
                    parsed_ual.knowledge_collection_id, parsed_ual.blockchain
                );
                tracing::error!(operation_id = %operation_id, %error_message, "UAL validation failed");
                self.get_operation_status_service
                    .mark_failed(operation_id, error_message)
                    .await;
                return CommandExecutionResult::Completed;
            }
            Err(e) => {
                // Log warning but continue - collection might be on old storage contract
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to validate UAL on-chain, continuing anyway"
                );
            }
        }

        let token_ids = self.resolve_token_ids(operation_id, &parsed_ual).await;

        // Try local triple store query first (local-first pattern)
        let local_result = self
            .triple_store_service
            .query_assertion(
                &parsed_ual,
                &token_ids,
                data.visibility,
                data.include_metadata,
            )
            .await;

        match local_result {
            Ok(Some(result)) if result.assertion.has_data() => {
                tracing::debug!(
                    operation_id = %operation_id,
                    public_count = result.assertion.public.len(),
                    has_private = result.assertion.private.is_some(),
                    has_metadata = result.metadata.is_some(),
                    "Found data locally, validating..."
                );

                // Validate local result
                let is_valid = self
                    .get_validation_service
                    .validate_response(&result.assertion, &parsed_ual, data.visibility)
                    .await;

                if is_valid {
                    tracing::info!(
                        operation_id = %operation_id,
                        public_count = result.assertion.public.len(),
                        has_private = result.assertion.private.is_some(),
                        has_metadata = result.metadata.is_some(),
                        "Local data validated, completing operation"
                    );

                    // Build the assertion from local data and store result
                    let assertion = Assertion {
                        public: result.assertion.public.clone(),
                        private: result.assertion.private.clone(),
                    };
                    let get_result = GetOperationResult::new(assertion, result.metadata.clone());

                    if let Err(e) = self
                        .get_operation_status_service
                        .store_result(operation_id, &get_result)
                    {
                        let error_message = format!("Failed to store local result: {}", e);
                        tracing::error!(operation_id = %operation_id, error = %e, "Failed to store local result");
                        self.get_operation_status_service
                            .mark_failed(operation_id, error_message)
                            .await;
                        return CommandExecutionResult::Completed;
                    }

                    // Mark operation as completed (local-first success: 1 success, 0 failures)
                    if let Err(e) = self
                        .get_operation_status_service
                        .mark_completed(operation_id)
                        .await
                    {
                        tracing::error!(
                            operation_id = %operation_id,
                            error = %e,
                            "Failed to mark operation as completed"
                        );
                    }

                    return CommandExecutionResult::Completed;
                } else {
                    tracing::debug!(
                        operation_id = %operation_id,
                        "Local data validation failed, querying network"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Local triple store query failed; falling back to network"
                );
            }
            Ok(_) => {}
        }

        tracing::debug!(
            operation_id = %operation_id,
            "Data not found locally, querying network"
        );

        let mut peers = match self
            .load_shard_peers(operation_id, &parsed_ual, data.paranet_ual.as_deref())
            .await
        {
            Ok(peers) => peers,
            Err(result) => return result,
        };

        // Prefer peers with better historical response times.
        self.peer_performance_tracker.sort_by_latency(&mut peers);

        let min_required_peers =
            protocols::get::MIN_PEERS.max(protocols::get::MIN_ACK_RESPONSES as usize);
        let total_peers = peers.len();

        tracing::info!(
            operation_id = %operation_id,
            total_peers = total_peers,
            has_paranet = data.paranet_ual.is_some(),
            "Filtered peer IDs for operation"
        );

        // Check if we have enough peers
        if peers.len() < min_required_peers {
            let error_message = format!(
                "Unable to find enough nodes for operation: {}. Found {} nodes, need at least {}",
                operation_id,
                peers.len(),
                min_required_peers
            );
            self.get_operation_status_service
                .mark_failed(operation_id, error_message)
                .await;
            return CommandExecutionResult::Completed;
        }

        // Build the get request data (reusing token_ids from local query)
        let get_request_data = GetRequestData::new(
            parsed_ual.blockchain.clone(),
            format!("{:?}", &parsed_ual.contract),
            parsed_ual.knowledge_collection_id,
            parsed_ual.knowledge_asset_id,
            ual.clone(),
            token_ids,
            data.include_metadata,
            data.paranet_ual.clone(),
        );

        // Send requests to peers with bounded concurrency and process responses as they arrive.
        let mut success_count: u16 = 0;
        let mut failure_count: u16 = 0;
        let min_ack_required = protocols::get::MIN_ACK_RESPONSES;
        let mut reached_threshold = false;

        if !peers.is_empty() {
            let mut futures = FuturesUnordered::new();
            let mut peers_iter = peers.iter().cloned();
            let limit = protocols::get::CONCURRENT_PEERS.max(1).min(peers.len());

            for _ in 0..limit {
                if let Some(peer) = peers_iter.next() {
                    futures.push(send_get_request_to_peer(
                        self,
                        peer,
                        operation_id,
                        get_request_data.clone(),
                        parsed_ual.clone(),
                        data.visibility,
                    ));
                }
            }

            while let Some((peer, outcome, elapsed)) = futures.next().await {
                match outcome {
                    Ok(true) => {
                        success_count += 1;
                        self.peer_performance_tracker.record_latency(&peer, elapsed);
                    }
                    Ok(false) => {
                        failure_count += 1;
                        tracing::debug!(
                            operation_id = %operation_id,
                            peer = %peer,
                            "Response validation failed, treating as NACK"
                        );
                    }
                    Err(e) => {
                        failure_count += 1;
                        self.peer_performance_tracker.record_failure(&peer);
                        tracing::debug!(
                            operation_id = %operation_id,
                            peer = %peer,
                            error = %e,
                            "Request failed"
                        );
                    }
                }

                if success_count >= min_ack_required {
                    reached_threshold = true;
                    break;
                }

                if let Some(peer) = peers_iter.next() {
                    futures.push(send_get_request_to_peer(
                        self,
                        peer,
                        operation_id,
                        get_request_data.clone(),
                        parsed_ual.clone(),
                        data.visibility,
                    ));
                }
            }
        }

        if reached_threshold {
            tracing::info!(
                operation_id = %operation_id,
                success_count = success_count,
                failure_count = failure_count,
                "Get operation completed - success threshold reached"
            );

            if let Err(e) = self
                .get_operation_status_service
                .mark_completed(operation_id)
                .await
            {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to mark operation as completed"
                );
            }

            return CommandExecutionResult::Completed;
        }

        // All peer chunks exhausted without meeting success threshold
        let error_message = format!(
            "Failed to get data from network. Success: {}, Failed: {}, Required: {}",
            success_count,
            failure_count,
            protocols::get::MIN_ACK_RESPONSES
        );
        tracing::warn!(operation_id = %operation_id, %error_message);
        self.get_operation_status_service
            .mark_failed(operation_id, error_message)
            .await;

        CommandExecutionResult::Completed
    }
}

async fn send_get_request_to_peer(
    handler: &SendGetRequestsCommandHandler,
    peer: PeerId,
    operation_id: Uuid,
    request_data: GetRequestData,
    parsed_ual: ParsedUal,
    visibility: Visibility,
) -> (PeerId, Result<bool, NetworkError>, std::time::Duration) {
    let start = Instant::now();
    let addresses = handler
        .network_manager
        .get_peer_addresses(peer)
        .await
        .unwrap_or_default();
    let result = handler
        .network_manager
        .send_get_request(peer, addresses, operation_id, request_data)
        .await;
    let elapsed = start.elapsed();
    let outcome = match result {
        Ok(response) => {
            let is_valid = handler
                .validate_and_store_response(
                    operation_id,
                    &peer,
                    &response,
                    &parsed_ual,
                    visibility,
                )
                .await;
            Ok(is_valid)
        }
        Err(e) => Err(e),
    };
    (peer, outcome, elapsed)
}
