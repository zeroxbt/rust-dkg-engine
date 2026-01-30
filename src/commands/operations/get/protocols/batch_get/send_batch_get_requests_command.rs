/* use std::{collections::HashMap, sync::Arc};

use futures::future::join_all;
use libp2p::PeerId;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::BlockchainManager,
        network::{NetworkManager, messages::BatchGetRequestData},
        repository::RepositoryManager,
        triple_store::{MAX_TOKENS_PER_KC, TokenIds, Visibility},
    },
    operations::{BatchGetOperation, BatchGetOperationResult},
    services::{
        GetValidationService, TripleStoreService,
        operation::{Operation, OperationService as GenericOperationService},
    },
    types::{ParsedUal, parse_ual},
};

/// Command data for sending batch get requests to network nodes.
#[derive(Clone)]
pub(crate) struct SendBatchGetRequestsCommandData {
    pub operation_id: Uuid,
    pub uals: Vec<String>,
}

impl SendBatchGetRequestsCommandData {
    pub(crate) fn new(operation_id: Uuid, uals: Vec<String>) -> Self {
        Self { operation_id, uals }
    }
}

pub(crate) struct SendBatchGetRequestsCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_service: Arc<TripleStoreService>,
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager>,
    batch_get_operation_service: Arc<GenericOperationService<BatchGetOperation>>,
    get_validation_service: Arc<GetValidationService>,
}

impl SendBatchGetRequestsCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            batch_get_operation_service: Arc::clone(context.batch_get_operation_service()),
            get_validation_service: Arc::clone(context.get_validation_service()),
        }
    }
}

impl CommandHandler<SendBatchGetRequestsCommandData> for SendBatchGetRequestsCommandHandler {
    async fn execute(&self, data: &SendBatchGetRequestsCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;

        tracing::info!(
            operation_id = %operation_id,
            ual_count = data.uals.len(),
            "Starting SendBatchGetRequests command"
        );

        // 1. Parse and validate all UALs, get token IDs
        let mut parsed_uals: Vec<(String, ParsedUal, TokenIds)> = Vec::new();

        for ual in &data.uals {
            // Parse UAL
            let parsed_ual = match parse_ual(ual) {
                Ok(parsed_ual) => parsed_ual,
                Err(e) => {
                    tracing::warn!(
                        operation_id = %operation_id,
                        ual = %ual,
                        error = %e,
                        "Failed to parse UAL, skipping"
                    );
                    continue;
                }
            };

            let token_ids = if let Some(asset_id) = parsed_ual.knowledge_asset_id {
                TokenIds::single(asset_id as u64)
            } else {
                // Get token IDs from blockchain
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
                        tracing::warn!(
                            operation_id = %operation_id,
                            ual = %ual,
                            error = %e,
                            "Failed to get token IDs, using fallback"
                        );
                        None
                    }
                };

                match chain_range {
                    Some((global_start, global_end, global_burned)) => {
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
                    None => TokenIds::single(1),
                }
            };

            parsed_uals.push((ual.clone(), parsed_ual, token_ids));
        }

        if parsed_uals.is_empty() {
            let error_message = "No valid UALs provided".to_string();
            tracing::error!(operation_id = %operation_id, %error_message);
            self.batch_get_operation_service
                .mark_failed(operation_id, error_message)
                .await;
            return CommandExecutionResult::Completed;
        }

        // 2. Query local triple store in batch
        let uals_with_token_ids: Vec<(ParsedUal, TokenIds)> = parsed_uals
            .iter()
            .map(|(_, parsed, token_ids)| (parsed.clone(), token_ids.clone()))
            .collect();

        let local_results = self
            .triple_store_service
            .query_assertions_batch(&uals_with_token_ids, Visibility::All, true)
            .await;

        let local_results = match local_results {
            Ok(results) => results,
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Local batch query failed; falling back to network"
                );
                HashMap::new()
            }
        };

        tracing::debug!(
            operation_id = %operation_id,
            local_found = local_results.len(),
            "Local batch query completed"
        );

        // 3. Separate found locally vs need network
        let mut final_result = BatchGetOperationResult::empty();
        let mut uals_not_found: Vec<(String, ParsedUal, TokenIds)> = Vec::new();

        for (ual, parsed_ual, token_ids) in parsed_uals {
            if let Some(result) = local_results.get(&ual) {
                // Validate local result
                let assertion = result.assertion.clone();
                let is_valid = self
                    .get_validation_service
                    .validate_response(&assertion, &parsed_ual, Visibility::All)
                    .await;

                if is_valid {
                    final_result.local.push(ual.clone());
                    if let Some(metadata) = &result.metadata {
                        final_result.metadata.insert(ual.clone(), metadata.clone());
                    }
                    tracing::debug!(
                        operation_id = %operation_id,
                        ual = %ual,
                        "Found and validated locally"
                    );
                } else {
                    tracing::debug!(
                        operation_id = %operation_id,
                        ual = %ual,
                        "Local validation failed, will query network"
                    );
                    uals_not_found.push((ual, parsed_ual, token_ids));
                }
            } else {
                uals_not_found.push((ual, parsed_ual, token_ids));
            }
        }

        // 4. If all found locally, complete
        if uals_not_found.is_empty() {
            tracing::info!(
                operation_id = %operation_id,
                local_count = final_result.local.len(),
                "All UALs found locally, completing operation"
            );

            if let Err(e) = self
                .batch_get_operation_service
                .store_result(operation_id, &final_result)
            {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to store batch get result"
                );
            }

            if let Err(e) = self
                .batch_get_operation_service
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
        }

        // 5. Get blockchain from first UAL (assuming all same blockchain)
        let blockchain = &uals_not_found[0].1.blockchain;

        // 6. Get shard nodes
        let shard_nodes = match self
            .repository_manager
            .shard_repository()
            .get_all_peer_records(blockchain.as_str())
            .await
        {
            Ok(nodes) => nodes,
            Err(e) => {
                let error_message = format!("Failed to get shard nodes: {}", e);
                tracing::error!(operation_id = %operation_id, error = %e, "Failed to get shard nodes");
                self.batch_get_operation_service
                    .mark_failed(operation_id, error_message)
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

        // Filter out self
        let my_peer_id = *self.network_manager.peer_id();
        let peers: Vec<PeerId> = shard_nodes
            .iter()
            .filter_map(|record| record.peer_id.parse().ok())
            .filter(|peer_id| *peer_id != my_peer_id)
            .collect();

        tracing::info!(
            operation_id = %operation_id,
            peer_count = peers.len(),
            uals_needed = uals_not_found.len(),
            "Querying network for missing UALs"
        );

        if peers.is_empty() {
            let error_message = "No peers available for batch get".to_string();
            tracing::error!(operation_id = %operation_id, %error_message);
            self.batch_get_operation_service
                .mark_failed(operation_id, error_message)
                .await;
            return CommandExecutionResult::Completed;
        }

        // 7. Build request data
        let uals_to_fetch: Vec<String> = uals_not_found
            .iter()
            .map(|(ual, _, _)| ual.clone())
            .collect();
        let token_ids_map: HashMap<String, TokenIds> = uals_not_found
            .iter()
            .map(|(ual, _, token_ids)| (ual.clone(), token_ids.clone()))
            .collect();

        // Keep parsed UALs for validation
        let parsed_uals_map: HashMap<String, ParsedUal> = uals_not_found
            .iter()
            .map(|(ual, parsed, _)| (ual.clone(), parsed.clone()))
            .collect();

        let batch_request = BatchGetRequestData::new(
            blockchain.to_string(),
            uals_to_fetch.clone(),
            token_ids_map,
            true,
        );

        // 8. Send requests to peers in chunks
        let mut uals_still_needed: Vec<String> = uals_to_fetch;

        for (chunk_idx, peer_chunk) in peers.chunks(BatchGetOperation::CONCURRENT_PEERS).enumerate() {
            if uals_still_needed.is_empty() {
                break;
            }

            tracing::debug!(
                operation_id = %operation_id,
                chunk = chunk_idx,
                peer_count = peer_chunk.len(),
                uals_needed = uals_still_needed.len(),
                "Sending requests to peer chunk"
            );

            // Send requests concurrently
            let request_futures: Vec<_> = peer_chunk
                .iter()
                .map(|peer| {
                    let peer = *peer;
                    let request_data = batch_request.clone();
                    let network_manager = Arc::clone(&self.network_manager);
                    async move {
                        // Get peer addresses from Kademlia for reliable request delivery
                        let addresses = network_manager
                            .get_peer_addresses(peer)
                            .await
                            .unwrap_or_default();
                        let result = network_manager
                            .send_batch_get_request(peer, addresses, operation_id, request_data)
                            .await;
                        (peer, result)
                    }
                })
                .collect();

            let results = join_all(request_futures).await;

            // Process responses
            for (peer, result) in results {
                match result {
                    Ok(response) => {
                        let metadata = response.metadata();

                        for (ual, assertion) in response.assertions() {
                            // Skip if already satisfied
                            if !uals_still_needed.contains(ual) {
                                continue;
                            }

                            // Validate the response
                            if let Some(parsed_ual) = parsed_uals_map.get(ual) {
                                let is_valid = self
                                    .get_validation_service
                                    .validate_response(assertion, parsed_ual, Visibility::All)
                                    .await;

                                if is_valid {
                                    final_result
                                        .remote
                                        .insert(ual.clone(), assertion.to_owned());
                                    if let Some(meta) = metadata.get(ual) {
                                        final_result.metadata.insert(ual.clone(), meta.clone());
                                    }
                                    uals_still_needed.retain(|u| u != ual);

                                    tracing::debug!(
                                        operation_id = %operation_id,
                                        ual = %ual,
                                        peer = %peer,
                                        "Received and validated from network"
                                    );
                                } else {
                                    tracing::debug!(
                                        operation_id = %operation_id,
                                        ual = %ual,
                                        peer = %peer,
                                        "Response validation failed"
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(
                            operation_id = %operation_id,
                            peer = %peer,
                            error = %e,
                            "Request failed"
                        );
                    }
                }
            }
        }

        // 9. Complete operation
        let total_found = final_result.local.len() + final_result.remote.len();
        let total_missing = uals_still_needed.len();

        tracing::info!(
            operation_id = %operation_id,
            local = final_result.local.len(),
            remote = final_result.remote.len(),
            missing = total_missing,
            "Batch get operation completed"
        );

        if let Err(e) = self
            .batch_get_operation_service
            .store_result(operation_id, &final_result)
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to store batch get result"
            );
        }

        if total_found > 0 {
            if let Err(e) = self
                .batch_get_operation_service
                .mark_completed(operation_id, 1, total_missing as u16)
                .await
            {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to mark operation as completed"
                );
            }
        } else {
            let error_message = format!(
                "Failed to retrieve any UALs. Missing: {}",
                uals_still_needed.join(", ")
            );
            self.batch_get_operation_service
                .mark_failed(operation_id, error_message)
                .await;
        }

        CommandExecutionResult::Completed
    }
}
 */
