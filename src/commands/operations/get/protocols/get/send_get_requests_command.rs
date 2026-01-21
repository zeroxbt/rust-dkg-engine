use std::sync::Arc;

use blockchain::BlockchainManager;
use futures::future::join_all;
use libp2p::PeerId;
use network::{
    NetworkManager, RequestMessage,
    message::{RequestMessageHeader, RequestMessageType},
};
use repository::RepositoryManager;
use uuid::Uuid;

use triple_store::{TokenIds, Visibility};

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    controllers::rpc_controller::{
        NetworkProtocols, ProtocolRequest,
        messages::GetRequestData,
    },
    services::{
        GetOperationContext, GetOperationContextStore, GetValidationService, OperationService,
        RequestTracker, TripleStoreService,
    },
    utils::ual::{ParsedUal, parse_ual},
};

/// Batch size for sending get requests to network nodes
const BATCH_SIZE: usize = 5;

/// Minimum number of ACK responses required for get operation
const MIN_ACK_RESPONSES: u8 = 1;

/// Command data for sending get requests to network nodes.
#[derive(Clone)]
pub struct SendGetRequestsCommandData {
    pub operation_id: Uuid,
    pub ual: String,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub visibility: Visibility,
}

impl SendGetRequestsCommandData {
    pub fn new(
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

pub struct SendGetRequestsCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_service: Arc<TripleStoreService>,
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    get_operation_manager: Arc<OperationService>,
    get_validation_service: Arc<GetValidationService>,
    get_operation_context_store: Arc<GetOperationContextStore>,
    request_tracker: Arc<RequestTracker>,
}

impl SendGetRequestsCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            get_operation_manager: Arc::clone(context.get_operation_manager()),
            get_validation_service: Arc::clone(context.get_validation_service()),
            get_operation_context_store: Arc::clone(context.get_operation_context_store()),
            request_tracker: Arc::clone(context.request_tracker()),
        }
    }

    /// Build token IDs from on-chain data or parsed UAL.
    ///
    /// If requesting a specific asset, returns a single token ID.
    /// If requesting entire collection, uses the on-chain range with burned tokens.
    fn build_token_ids_from_chain(
        parsed_ual: &ParsedUal,
        chain_range: Option<(u64, u64, Vec<u64>)>,
    ) -> TokenIds {
        match parsed_ual.knowledge_asset_id {
            Some(asset_id) => TokenIds::single(asset_id as u64),
            None => {
                // Use on-chain data if available
                match chain_range {
                    Some((start, end, burned)) => TokenIds::new(start, end, burned),
                    None => {
                        // Fallback for old ContentAssetStorage contracts
                        TokenIds::single(1)
                    }
                }
            }
        }
    }
}

// TODO: add get paranet UAL support
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
                self.get_operation_manager
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
                self.get_operation_manager
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

        // Get token IDs range from blockchain
        let chain_range = match self
            .blockchain_manager
            .get_knowledge_assets_range(&parsed_ual.blockchain, parsed_ual.knowledge_collection_id)
            .await
        {
            Ok(range) => {
                if let Some((start, end, ref burned)) = range {
                    tracing::debug!(
                        operation_id = %operation_id,
                        start_token_id = start,
                        end_token_id = end,
                        burned_count = burned.len(),
                        "Retrieved knowledge assets range from chain"
                    );
                }
                range
            }
            Err(e) => {
                // Fallback for old ContentAssetStorage contracts
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to get knowledge assets range, using fallback"
                );
                None
            }
        };

        // Build token IDs for local query
        let token_ids = Self::build_token_ids_from_chain(&parsed_ual, chain_range.clone());

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

        if let Some(result) = local_result
            && result.has_data()
        {
            tracing::debug!(
                operation_id = %operation_id,
                public_count = result.public.len(),
                has_private = result.private.is_some(),
                has_metadata = result.metadata.is_some(),
                "Found data locally, validating..."
            );

            // Validate local result
            let is_valid = self
                .get_validation_service
                .validate_response(
                    &result.public,
                    result.private.as_deref(),
                    &parsed_ual,
                    data.visibility,
                )
                .await;

            if is_valid {
                tracing::info!(
                    operation_id = %operation_id,
                    public_count = result.public.len(),
                    has_private = result.private.is_some(),
                    has_metadata = result.metadata.is_some(),
                    "Local data validated, completing operation"
                );

                // Build result JSON
                let result_json = serde_json::json!({
                    "assertion": {
                        "public": result.public,
                        "private": result.private
                    },
                    "metadata": result.metadata
                });

                // Mark operation as completed with result
                if let Err(e) = self
                    .get_operation_manager
                    .mark_completed_with_result(operation_id, result_json)
                    .await
                {
                    tracing::error!(
                        operation_id = %operation_id,
                        error = %e,
                        "Failed to mark operation as completed with local result"
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

        tracing::debug!(
            operation_id = %operation_id,
            "Data not found locally, querying network"
        );

        // Store operation context for response validation in RPC controller
        let context = GetOperationContext::new(
            parsed_ual.blockchain.clone(),
            parsed_ual.knowledge_collection_id,
            parsed_ual.knowledge_asset_id,
            data.visibility,
        );
        self.get_operation_context_store
            .store(operation_id, context);

        // Get shard nodes for the blockchain
        let shard_nodes = match self
            .repository_manager
            .shard_repository()
            .get_all_peer_records(parsed_ual.blockchain.as_str(), true)
            .await
        {
            Ok(nodes) => {
                tracing::info!(
                    operation_id = %operation_id,
                    shard_nodes_count = nodes.len(),
                    "Retrieved shard nodes from repository"
                );
                nodes
            }
            Err(e) => {
                let error_message = format!("Failed to get shard nodes: {}", e);
                tracing::error!(operation_id = %operation_id, error = %e, "Failed to get shard nodes");
                self.get_operation_manager
                    .mark_failed(operation_id, error_message)
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

        // Filter out self and parse peer IDs
        let my_peer_id = *self.network_manager.peer_id();
        let peers: Vec<PeerId> = shard_nodes
            .iter()
            .filter_map(|record| record.peer_id.parse().ok())
            .filter(|peer_id| *peer_id != my_peer_id)
            .collect();

        let total_peers = peers.len() as u16;

        tracing::info!(
            operation_id = %operation_id,
            total_peers = total_peers,
            "Filtered peer IDs (excluding self)"
        );

        // Check if we have enough peers
        if peers.len() < MIN_ACK_RESPONSES as usize {
            let error_message = format!(
                "Unable to find enough nodes for operation: {}. Found {} nodes, need at least {}",
                operation_id,
                peers.len(),
                MIN_ACK_RESPONSES
            );
            self.get_operation_manager
                .mark_failed(operation_id, error_message)
                .await;
            return CommandExecutionResult::Completed;
        }

        // Initialize progress tracking
        if let Err(e) = self
            .get_operation_manager
            .initialize_progress(operation_id, total_peers, MIN_ACK_RESPONSES as u16)
            .await
        {
            self.get_operation_manager
                .mark_failed(operation_id, e.to_string())
                .await;
            return CommandExecutionResult::Completed;
        }

        // Build the get request data (reusing token_ids from local query)
        let get_request_data = GetRequestData::new(
            ual.clone(),
            token_ids,
            data.include_metadata,
            data.paranet_ual.clone(),
            data.visibility,
        );

        // Send requests to peers in batches
        // Unlike publish, get operation returns on first valid response
        for batch in peers.chunks(BATCH_SIZE) {
            let mut send_futures = Vec::with_capacity(batch.len());

            for peer_id in batch {
                let network_manager = Arc::clone(&self.network_manager);
                let request_tracker = Arc::clone(&self.request_tracker);
                let peer = *peer_id;
                let op_id = operation_id;

                let message = RequestMessage {
                    header: RequestMessageHeader {
                        operation_id,
                        message_type: RequestMessageType::ProtocolRequest,
                    },
                    data: get_request_data.clone(),
                };

                send_futures.push(async move {
                    tracing::debug!(
                        operation_id = %op_id,
                        peer = %peer,
                        "Sending get request to peer"
                    );

                    match network_manager
                        .send_protocol_request(ProtocolRequest::Get { peer, message })
                        .await
                    {
                        Ok(request_id) => {
                            request_tracker.track(request_id, op_id, peer);
                            tracing::info!(
                                operation_id = %op_id,
                                peer = %peer,
                                ?request_id,
                                "Get request sent successfully"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                operation_id = %op_id,
                                peer = %peer,
                                error = %e,
                                "Failed to send get request"
                            );
                        }
                    }
                });
            }

            // Wait for batch to complete
            join_all(send_futures).await;

            tracing::debug!(
                operation_id = %operation_id,
                batch_size = batch.len(),
                "Batch of get requests sent"
            );

            // Note: In the JS implementation, we would check if any valid response
            // was received after each batch and return early. For now, we send all
            // batches and let the RPC controller handle responses.
            // The operation_manager will mark the operation as completed on first
            // successful response via record_response(operation_id, true).
        }

        tracing::info!(
            operation_id = %operation_id,
            total_peers = total_peers,
            "All get requests have been sent"
        );

        CommandExecutionResult::Completed
    }
}
