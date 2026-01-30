use std::{collections::HashSet, sync::Arc};

use futures::future::join_all;
use libp2p::PeerId;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::BlockchainManager,
        network::{
            NetworkManager,
            message::ResponseBody,
            messages::{GetRequestData, GetResponseData},
        },
        repository::RepositoryManager,
        triple_store::{Assertion, MAX_TOKENS_PER_KC, TokenIds},
    },
    operations::{GetOperation, GetOperationResult},
    services::{
        GetValidationService, TripleStoreService,
        operation::{Operation, OperationService as GenericOperationService},
    },
    types::{AccessPolicy, ParsedUal, Visibility, parse_ual},
    utils::paranet::{construct_knowledge_collection_onchain_id, construct_paranet_id},
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
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_service: Arc<TripleStoreService>,
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager>,
    get_operation_service: Arc<GenericOperationService<GetOperation>>,
    get_validation_service: Arc<GetValidationService>,
}

impl SendGetRequestsCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            get_operation_service: Arc::clone(context.get_operation_service()),
            get_validation_service: Arc::clone(context.get_validation_service()),
        }
    }

    /// Handle paranet validation and return filtered peers based on access policy.
    ///
    /// For PERMISSIONED paranets, only returns nodes that are in the permissioned list.
    /// For OPEN paranets, returns all provided peers.
    ///
    /// Returns Ok(peers) for valid paranets, or Err(()) if validation fails
    /// (operation is marked as failed in that case).
    async fn handle_paranet_node_selection(
        &self,
        operation_id: Uuid,
        paranet_ual: &str,
        target_ual: &ParsedUal,
        all_shard_peers: Vec<PeerId>,
    ) -> Result<Vec<PeerId>, ()> {
        // 1. Parse paranet UAL
        let paranet_parsed = match parse_ual(paranet_ual) {
            Ok(p) => p,
            Err(e) => {
                let error_message = format!("Invalid paranet UAL: {}", e);
                tracing::error!(operation_id = %operation_id, %error_message);
                self.get_operation_service
                    .mark_failed(operation_id, error_message)
                    .await;
                return Err(());
            }
        };

        // 2. Validate paranet UAL has knowledge_asset_id
        let Some(ka_id) = paranet_parsed.knowledge_asset_id else {
            let error_message = "Paranet UAL must include knowledge asset ID".to_string();
            tracing::error!(operation_id = %operation_id, %error_message);
            self.get_operation_service
                .mark_failed(operation_id, error_message)
                .await;
            return Err(());
        };

        // 3. Construct paranet ID
        let paranet_id = construct_paranet_id(
            paranet_parsed.contract,
            paranet_parsed.knowledge_collection_id,
            ka_id,
        );

        tracing::debug!(
            operation_id = %operation_id,
            paranet_id = %paranet_id,
            "Constructed paranet ID"
        );

        // 4. Check paranet exists
        let exists = self
            .blockchain_manager
            .paranet_exists(&target_ual.blockchain, paranet_id)
            .await
            .unwrap_or(false);

        if !exists {
            let error_message = format!("Paranet does not exist: {}", paranet_ual);
            tracing::error!(operation_id = %operation_id, %error_message);
            self.get_operation_service
                .mark_failed(operation_id, error_message)
                .await;
            return Err(());
        }

        // 5. Get access policy
        let policy = match self
            .blockchain_manager
            .get_nodes_access_policy(&target_ual.blockchain, paranet_id)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                let error_message = format!("Failed to get access policy: {}", e);
                tracing::error!(operation_id = %operation_id, %error_message);
                self.get_operation_service
                    .mark_failed(operation_id, error_message)
                    .await;
                return Err(());
            }
        };

        tracing::debug!(
            operation_id = %operation_id,
            policy = ?policy,
            "Retrieved paranet access policy"
        );

        // 6. Check KC is registered in paranet
        let kc_onchain_id = construct_knowledge_collection_onchain_id(
            target_ual.contract,
            target_ual.knowledge_collection_id,
        );
        let kc_registered = self
            .blockchain_manager
            .is_knowledge_collection_registered(&target_ual.blockchain, paranet_id, kc_onchain_id)
            .await
            .unwrap_or(false);

        if !kc_registered {
            let error_message = "Knowledge collection not registered in paranet".to_string();
            tracing::error!(operation_id = %operation_id, %error_message);
            self.get_operation_service
                .mark_failed(operation_id, error_message)
                .await;
            return Err(());
        }

        // 7. Filter peers based on policy
        match policy {
            AccessPolicy::Permissioned => {
                let permissioned_nodes = match self
                    .blockchain_manager
                    .get_permissioned_nodes(&target_ual.blockchain, paranet_id)
                    .await
                {
                    Ok(nodes) => nodes,
                    Err(e) => {
                        let error_message = format!("Failed to get permissioned nodes: {}", e);
                        tracing::error!(operation_id = %operation_id, %error_message);
                        self.get_operation_service
                            .mark_failed(operation_id, error_message)
                            .await;
                        return Err(());
                    }
                };

                // Convert node_id bytes to peer IDs
                let permissioned_peer_ids: HashSet<PeerId> = permissioned_nodes
                    .iter()
                    .filter_map(|node| {
                        String::from_utf8(node.nodeId.to_vec())
                            .ok()
                            .and_then(|s| s.parse::<PeerId>().ok())
                    })
                    .collect();

                tracing::debug!(
                    operation_id = %operation_id,
                    permissioned_count = permissioned_peer_ids.len(),
                    "Retrieved permissioned nodes for paranet"
                );

                let my_peer_id = *self.network_manager.peer_id();
                let filtered: Vec<PeerId> = all_shard_peers
                    .into_iter()
                    .filter(|peer_id| {
                        *peer_id != my_peer_id && permissioned_peer_ids.contains(peer_id)
                    })
                    .collect();

                tracing::info!(
                    operation_id = %operation_id,
                    filtered_count = filtered.len(),
                    "Filtered to permissioned nodes only"
                );

                Ok(filtered)
            }
            AccessPolicy::Open => {
                tracing::debug!(
                    operation_id = %operation_id,
                    "Paranet is OPEN, using all shard nodes"
                );
                Ok(all_shard_peers)
            }
        }
    }

    /// Validate a get response and store the result if valid.
    ///
    /// Returns true if the response is valid and was stored successfully.
    async fn validate_and_store_response(
        &self,
        operation_id: Uuid,
        peer: &PeerId,
        response: &GetResponseData,
        parsed_ual: &ParsedUal,
        visibility: Visibility,
    ) -> bool {
        match response {
            ResponseBody::Ack(ack) => {
                let assertion = &ack.assertion;
                let metadata = &ack.metadata;
                // Validate the assertion
                let is_valid = self
                    .get_validation_service
                    .validate_response(assertion, parsed_ual, visibility)
                    .await;

                if !is_valid {
                    tracing::debug!(
                        operation_id = %operation_id,
                        peer = %peer,
                        "Response validation failed"
                    );
                    return false;
                }

                // Build and store the result
                let get_result = GetOperationResult::new(
                    Assertion::new(assertion.public.clone(), assertion.private.clone()),
                    metadata.clone(),
                );

                match self
                    .get_operation_service
                    .store_result(operation_id, &get_result)
                {
                    Ok(()) => {
                        tracing::debug!(
                            operation_id = %operation_id,
                            peer = %peer,
                            public_count = assertion.public.len(),
                            has_private = assertion.private.is_some(),
                            "Response validated and stored"
                        );
                        true
                    }
                    Err(e) => {
                        tracing::error!(
                            operation_id = %operation_id,
                            peer = %peer,
                            error = %e,
                            "Failed to store result"
                        );
                        false
                    }
                }
            }
            ResponseBody::Error(err) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    error = %err.error_message,
                    "Peer returned error response"
                );
                false
            }
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
                self.get_operation_service
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
                self.get_operation_service
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

        let token_ids = if let Some(token_id) = parsed_ual.knowledge_asset_id {
            TokenIds::single(token_id as u64)
        } else {
            // Get token IDs range from blockchain
            let chain_range = match self
                .blockchain_manager
                .get_knowledge_assets_range(
                    &parsed_ual.blockchain,
                    parsed_ual.contract,
                    parsed_ual.knowledge_collection_id,
                )
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

            // Use on-chain data if available
            match chain_range {
                Some((global_start, global_end, global_burned)) => {
                    // Convert global token IDs to local 1-based indices
                    // Global: (kc_id - 1) * 1_000_000 + local_id
                    // Local: global - (kc_id - 1) * 1_000_000
                    let offset =
                        (parsed_ual.knowledge_collection_id as u64 - 1) * MAX_TOKENS_PER_KC;
                    let local_start = global_start.saturating_sub(offset);
                    let local_end = global_end.saturating_sub(offset);
                    let local_burned: Vec<u64> = global_burned
                        .into_iter()
                        .map(|b| b.saturating_sub(offset))
                        .collect();
                    TokenIds::new(local_start, local_end, local_burned)
                }
                None => {
                    // Fallback for old ContentAssetStorage contracts
                    TokenIds::single(1)
                }
            }
        };

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
                        .get_operation_service
                        .store_result(operation_id, &get_result)
                    {
                        let error_message = format!("Failed to store local result: {}", e);
                        tracing::error!(operation_id = %operation_id, error = %e, "Failed to store local result");
                        self.get_operation_service
                            .mark_failed(operation_id, error_message)
                            .await;
                        return CommandExecutionResult::Completed;
                    }

                    // Mark operation as completed (local-first success: 1 success, 0 failures)
                    if let Err(e) = self
                        .get_operation_service
                        .mark_completed(operation_id, 1, 0)
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

        // Get shard nodes for the blockchain
        let shard_nodes = match self
            .repository_manager
            .shard_repository()
            .get_all_peer_records(parsed_ual.blockchain.as_str())
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
                self.get_operation_service
                    .mark_failed(operation_id, error_message)
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

        // Filter out self and parse peer IDs
        let my_peer_id = *self.network_manager.peer_id();
        let all_shard_peers: Vec<PeerId> = shard_nodes
            .iter()
            .filter_map(|record| record.peer_id.parse().ok())
            .filter(|peer_id| *peer_id != my_peer_id)
            .collect();

        // Apply paranet filtering if paranet_ual is provided
        let peers: Vec<PeerId> = if let Some(ref paranet_ual) = data.paranet_ual {
            tracing::info!(
                operation_id = %operation_id,
                paranet_ual = %paranet_ual,
                "Applying paranet node filtering"
            );
            match self
                .handle_paranet_node_selection(
                    operation_id,
                    paranet_ual,
                    &parsed_ual,
                    all_shard_peers,
                )
                .await
            {
                Ok(filtered_peers) => filtered_peers,
                Err(()) => {
                    // Operation already marked as failed in handle_paranet_node_selection
                    return CommandExecutionResult::Completed;
                }
            }
        } else {
            all_shard_peers
        };

        let total_peers = peers.len() as u16;

        tracing::info!(
            operation_id = %operation_id,
            total_peers = total_peers,
            has_paranet = data.paranet_ual.is_some(),
            "Filtered peer IDs for operation"
        );

        // Check if we have enough peers
        if peers.len() < GetOperation::MIN_ACK_RESPONSES as usize {
            let error_message = format!(
                "Unable to find enough nodes for operation: {}. Found {} nodes, need at least {}",
                operation_id,
                peers.len(),
                GetOperation::MIN_ACK_RESPONSES
            );
            self.get_operation_service
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

        // Send requests to peers in chunks and process responses directly
        let mut success_count: u16 = 0;
        let mut failure_count: u16 = 0;

        for (chunk_idx, peer_chunk) in peers.chunks(GetOperation::CONCURRENT_PEERS).enumerate() {
            tracing::debug!(
                operation_id = %operation_id,
                chunk = chunk_idx,
                peer_count = peer_chunk.len(),
                "Sending get requests to peer chunk"
            );

            // Send all requests in this chunk concurrently
            let request_futures: Vec<_> = peer_chunk
                .iter()
                .map(|peer| {
                    let peer = *peer;
                    let request_data = get_request_data.clone();
                    let network_manager = Arc::clone(&self.network_manager);
                    async move {
                        // Get peer addresses from Kademlia for reliable request delivery
                        let addresses = network_manager
                            .get_peer_addresses(peer)
                            .await
                            .unwrap_or_default();
                        let result = network_manager
                            .send_get_request(peer, addresses, operation_id, request_data)
                            .await;
                        (peer, result)
                    }
                })
                .collect();

            // Wait for all requests in this chunk to complete (success or failure)
            let results = join_all(request_futures).await;

            // Process each response
            for (peer, result) in results {
                match result {
                    Ok(response) => {
                        // Validate the response
                        let is_valid = self
                            .validate_and_store_response(
                                operation_id,
                                &peer,
                                &response,
                                &parsed_ual,
                                data.visibility,
                            )
                            .await;

                        if is_valid {
                            success_count += 1;

                            // Check if we've met the success threshold
                            if success_count >= GetOperation::MIN_ACK_RESPONSES {
                                tracing::info!(
                                    operation_id = %operation_id,
                                    success_count = success_count,
                                    failure_count = failure_count,
                                    chunk = chunk_idx,
                                    "Get operation completed - success threshold reached"
                                );

                                // Mark operation as completed with final counts
                                if let Err(e) = self
                                    .get_operation_service
                                    .mark_completed(operation_id, success_count, failure_count)
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
                        } else {
                            failure_count += 1;
                            tracing::debug!(
                                operation_id = %operation_id,
                                peer = %peer,
                                "Response validation failed, treating as NACK"
                            );
                        }
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
            }

            tracing::debug!(
                operation_id = %operation_id,
                chunk = chunk_idx,
                success_count = success_count,
                failure_count = failure_count,
                "Peer chunk completed"
            );
        }

        // All peer chunks exhausted without meeting success threshold
        let error_message = format!(
            "Failed to get data from network. Success: {}, Failed: {}, Required: {}",
            success_count,
            failure_count,
            GetOperation::MIN_ACK_RESPONSES
        );
        tracing::warn!(operation_id = %operation_id, %error_message);
        self.get_operation_service
            .mark_failed(operation_id, error_message)
            .await;

        CommandExecutionResult::Completed
    }
}
