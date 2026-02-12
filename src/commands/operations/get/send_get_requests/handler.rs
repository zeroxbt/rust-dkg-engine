use std::sync::Arc;

use futures::{StreamExt, stream::FuturesUnordered};
use libp2p::PeerId;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    commands::{executor::CommandOutcome, registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::BlockchainManager,
        network::{NetworkError, NetworkManager, messages::GetRequestData},
    },
    operations::{GetOperation, GetOperationResult},
    services::{
        AssertionValidationService, PeerService, TripleStoreService,
        operation_status::OperationStatusService as GenericOperationService,
    },
    types::{Assertion, ParsedUal, TokenIds, Visibility, parse_ual},
};

/// Maximum number of in-flight peer requests for this operation.
pub(crate) const CONCURRENT_PEERS: usize = 3;

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
    pub(super) network_manager: Arc<NetworkManager>,
    pub(super) get_operation_status_service: Arc<GenericOperationService<GetOperation>>,
    pub(super) assertion_validation_service: Arc<AssertionValidationService>,
    pub(super) peer_service: Arc<PeerService>,
}

impl SendGetRequestsCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            network_manager: Arc::clone(context.network_manager()),
            get_operation_status_service: Arc::clone(context.get_operation_status_service()),
            assertion_validation_service: Arc::clone(context.assertion_validation_service()),
            peer_service: Arc::clone(context.peer_service()),
        }
    }
}

impl CommandHandler<SendGetRequestsCommandData> for SendGetRequestsCommandHandler {
    #[instrument(
        name = "op.get.send",
        skip(self, data),
        fields(
            operation_id = %data.operation_id,
            protocol = "get",
            direction = "send",
            ual = %data.ual,
            paranet = ?data.paranet_ual,
            visibility = ?data.visibility,
            include_metadata = data.include_metadata,
            blockchain = tracing::field::Empty,
            peer_count = tracing::field::Empty,
        )
    )]
    async fn execute(&self, data: &SendGetRequestsCommandData) -> CommandOutcome {
        let operation_id = data.operation_id;
        let ual = &data.ual;

        let parsed_ual = match self.parse_ual_phase(operation_id, ual).await {
            Ok(parsed) => parsed,
            Err(result) => return result,
        };
        tracing::Span::current().record(
            "blockchain",
            tracing::field::display(&parsed_ual.blockchain),
        );

        if let Err(result) = self.validate_onchain_phase(operation_id, &parsed_ual).await {
            return result;
        }

        let token_ids = self.resolve_token_ids(operation_id, &parsed_ual).await;

        match self
            .local_query_phase(
                operation_id,
                &parsed_ual,
                &token_ids,
                data.visibility,
                data.include_metadata,
            )
            .await
        {
            Ok(LocalQueryOutcome::Completed) => return CommandOutcome::Completed,
            Ok(LocalQueryOutcome::Continue) => {}
            Err(result) => return result,
        }

        tracing::debug!(
            operation_id = %operation_id,
            "Local cache miss; querying network"
        );

        self.network_query_phase(operation_id, ual, &parsed_ual, token_ids, data)
            .await
    }
}

enum LocalQueryOutcome {
    Completed,
    Continue,
}

impl SendGetRequestsCommandHandler {
    #[instrument(
        name = "op.get.send.parse",
        skip(self, ual),
        fields(operation_id = %operation_id, ual = %ual)
    )]
    async fn parse_ual_phase(
        &self,
        operation_id: Uuid,
        ual: &str,
    ) -> Result<ParsedUal, CommandOutcome> {
        match parse_ual(ual) {
            Ok(parsed) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    blockchain = %parsed.blockchain,
                    contract = ?parsed.contract,
                    kc_id = parsed.knowledge_collection_id,
                    ka_id = ?parsed.knowledge_asset_id,
                    "Parsed UAL"
                );
                Ok(parsed)
            }
            Err(e) => {
                let error_message = format!("Invalid UAL format: {}", e);
                tracing::error!(operation_id = %operation_id, error = %e, "Failed to parse UAL");
                self.get_operation_status_service
                    .mark_failed(operation_id, error_message)
                    .await;
                Err(CommandOutcome::Completed)
            }
        }
    }

    #[instrument(
        name = "op.get.send.validate_onchain",
        skip(self, parsed_ual),
        fields(
            operation_id = %operation_id,
            blockchain = %parsed_ual.blockchain,
            kc_id = parsed_ual.knowledge_collection_id,
        )
    )]
    async fn validate_onchain_phase(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
    ) -> Result<(), CommandOutcome> {
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
                    "Knowledge collection validated on-chain"
                );
                Ok(())
            }
            Ok(None) => {
                let error_message = format!(
                    "Knowledge collection {} does not exist on blockchain {}",
                    parsed_ual.knowledge_collection_id, parsed_ual.blockchain
                );
                tracing::error!(
                    operation_id = %operation_id,
                    %error_message,
                    "UAL validation failed"
                );
                self.get_operation_status_service
                    .mark_failed(operation_id, error_message)
                    .await;
                Err(CommandOutcome::Completed)
            }
            Err(e) => {
                // Log warning but continue - collection might be on old storage contract
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to validate UAL on-chain, continuing anyway"
                );
                Ok(())
            }
        }
    }

    #[instrument(
        name = "op.get.send.local",
        skip(self, parsed_ual, token_ids),
        fields(
            operation_id = %operation_id,
            visibility = ?visibility,
            include_metadata = include_metadata,
        )
    )]
    async fn local_query_phase(
        &self,
        operation_id: Uuid,
        parsed_ual: &ParsedUal,
        token_ids: &TokenIds,
        visibility: Visibility,
        include_metadata: bool,
    ) -> Result<LocalQueryOutcome, CommandOutcome> {
        let local_result = self
            .triple_store_service
            .query_assertion(parsed_ual, token_ids, visibility, include_metadata)
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

                let is_valid = self
                    .assertion_validation_service
                    .validate_response(&result.assertion, parsed_ual, visibility)
                    .await;

                if is_valid {
                    tracing::info!(
                        operation_id = %operation_id,
                        source = "local",
                        public_count = result.assertion.public.len(),
                        has_private = result.assertion.private.is_some(),
                        has_metadata = result.metadata.is_some(),
                        "Get operation completed"
                    );

                    let assertion = Assertion {
                        public: result.assertion.public.clone(),
                        private: result.assertion.private.clone(),
                    };
                    let get_result = GetOperationResult::new(assertion, result.metadata.clone());

                    if let Err(e) = self
                        .get_operation_status_service
                        .complete_with_result(operation_id, get_result)
                        .await
                    {
                        tracing::error!(
                            operation_id = %operation_id,
                            error = %e,
                            "Failed to complete operation with local result"
                        );
                        self.get_operation_status_service
                            .mark_failed(operation_id, e.to_string())
                            .await;
                        return Err(CommandOutcome::Completed);
                    }

                    Ok(LocalQueryOutcome::Completed)
                } else {
                    tracing::debug!(
                        operation_id = %operation_id,
                        "Local validation failed; querying network"
                    );
                    Ok(LocalQueryOutcome::Continue)
                }
            }
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Local triple store query failed; falling back to network"
                );
                Ok(LocalQueryOutcome::Continue)
            }
            Ok(_) => Ok(LocalQueryOutcome::Continue),
        }
    }

    #[instrument(
        name = "op.get.send.network",
        skip(self, parsed_ual, token_ids, data),
        fields(
            operation_id = %operation_id,
            has_paranet = data.paranet_ual.is_some(),
            peer_count = tracing::field::Empty,
        )
    )]
    async fn network_query_phase(
        &self,
        operation_id: Uuid,
        ual: &str,
        parsed_ual: &ParsedUal,
        token_ids: TokenIds,
        data: &SendGetRequestsCommandData,
    ) -> CommandOutcome {
        let mut peers = match self
            .load_shard_peers(operation_id, parsed_ual, data.paranet_ual.as_deref())
            .await
        {
            Ok(peers) => peers,
            Err(result) => return result,
        };
        tracing::Span::current().record("peer_count", tracing::field::display(peers.len()));

        self.peer_service.sort_by_latency(&mut peers);

        let min_required_peers = 1;
        let total_peers = peers.len();

        tracing::debug!(
            operation_id = %operation_id,
            total_peers = total_peers,
            has_paranet = data.paranet_ual.is_some(),
            "Selected peers for network query"
        );

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
            return CommandOutcome::Completed;
        }

        let get_request_data = GetRequestData::new(
            parsed_ual.blockchain.clone(),
            format!("{:?}", &parsed_ual.contract),
            parsed_ual.knowledge_collection_id,
            parsed_ual.knowledge_asset_id,
            ual.to_string(),
            token_ids,
            data.include_metadata,
            data.paranet_ual.clone(),
        );

        let mut failure_count: u16 = 0;

        if !peers.is_empty() {
            let mut futures = FuturesUnordered::new();
            let mut peers_iter = peers.iter().cloned();
            let limit = CONCURRENT_PEERS.max(1).min(peers.len());

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

            while let Some((peer, outcome)) = futures.next().await {
                match outcome {
                    Ok(Some(get_result)) => {
                        tracing::info!(
                            operation_id = %operation_id,
                            source = "network",
                            success_count = 1,
                            failure_count = failure_count,
                            "Get operation completed"
                        );

                        if let Err(e) = self
                            .get_operation_status_service
                            .complete_with_result(operation_id, get_result)
                            .await
                        {
                            tracing::error!(
                                operation_id = %operation_id,
                                error = %e,
                                "Failed to complete operation with network result"
                            );
                            self.get_operation_status_service
                                .mark_failed(operation_id, e.to_string())
                                .await;
                        }

                        return CommandOutcome::Completed;
                    }
                    Ok(None) => {
                        failure_count += 1;
                        tracing::debug!(
                            operation_id = %operation_id,
                            peer = %peer,
                            "Response validation failed, treating as NACK"
                        );
                    }
                    Err(e) => {
                        failure_count += 1;
                        tracing::debug!(
                            operation_id = %operation_id,
                            peer = %peer,
                            error = %e,
                            "Request failed"
                        );
                    }
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

        let error_message = format!(
            "Failed to get data from network. Success: 0, Failed: {}, Required: 1",
            failure_count
        );
        tracing::warn!(operation_id = %operation_id, %error_message, "Get operation failed");
        self.get_operation_status_service
            .mark_failed(operation_id, error_message)
            .await;

        CommandOutcome::Completed
    }
}

async fn send_get_request_to_peer(
    handler: &SendGetRequestsCommandHandler,
    peer: PeerId,
    operation_id: Uuid,
    request_data: GetRequestData,
    parsed_ual: ParsedUal,
    visibility: Visibility,
) -> (PeerId, Result<Option<GetOperationResult>, NetworkError>) {
    let result = handler
        .network_manager
        .send_get_request(peer, operation_id, request_data)
        .await;
    let outcome = match result {
        Ok(response) => {
            let get_result = handler
                .validate_response(operation_id, &peer, &response, &parsed_ual, visibility)
                .await;
            Ok(get_result)
        }
        Err(e) => Err(e),
    };
    (peer, outcome)
}
