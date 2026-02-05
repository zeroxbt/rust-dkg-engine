use std::sync::Arc;

use futures::{StreamExt, stream::FuturesUnordered};
use libp2p::PeerId;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::{BlockchainId, BlockchainManager},
        network::{
            NetworkError, NetworkManager,
            messages::{StoreRequestData, StoreResponseData},
        },
        repository::RepositoryManager,
        triple_store::Assertion,
    },
    operations::{PublishStoreOperationResult, protocols},
    services::{
        operation_status::OperationStatusService as GenericOperationService,
        pending_storage_service::PendingStorageService,
    },
};

/// Command data for sending publish store requests to network nodes.
/// Dataset is passed inline instead of being retrieved from storage.
#[derive(Clone)]
pub(crate) struct SendPublishStoreRequestsCommandData {
    pub operation_id: Uuid,
    pub blockchain: BlockchainId,
    pub dataset_root: String,
    pub min_ack_responses: u8,
    pub dataset: Assertion,
}

impl SendPublishStoreRequestsCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        blockchain: BlockchainId,
        dataset_root: String,
        min_ack_responses: u8,
        dataset: Assertion,
    ) -> Self {
        Self {
            operation_id,
            blockchain,
            dataset_root,
            min_ack_responses,
            dataset,
        }
    }
}

pub(crate) struct SendPublishStoreRequestsCommandHandler {
    pub(super) repository_manager: Arc<RepositoryManager>,
    pub(super) network_manager: Arc<NetworkManager>,
    pub(super) blockchain_manager: Arc<BlockchainManager>,
    pub(super) publish_store_operation_status_service:
        Arc<GenericOperationService<PublishStoreOperationResult>>,
    pub(super) pending_storage_service: Arc<PendingStorageService>,
}

impl SendPublishStoreRequestsCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            publish_store_operation_status_service: Arc::clone(
                context.publish_store_operation_status_service(),
            ),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
        }
    }
}

impl CommandHandler<SendPublishStoreRequestsCommandData>
    for SendPublishStoreRequestsCommandHandler
{
    #[instrument(
        name = "op.publish_store.send",
        skip(self, data),
        fields(
            operation_id = %data.operation_id,
            protocol = "publish_store",
            direction = "send",
            blockchain = %data.blockchain,
            dataset_root = %data.dataset_root,
            min_ack_responses = data.min_ack_responses,
            dataset_public_len = data.dataset.public.len(),
            peer_count = tracing::field::Empty,
        )
    )]
    async fn execute(&self, data: &SendPublishStoreRequestsCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;
        let blockchain = &data.blockchain;
        let dataset_root = &data.dataset_root;
        let dataset = &data.dataset;

        // Determine effective min_ack_responses using max(default, chain_min, user_provided)
        let default_min = protocols::publish_store::MIN_ACK_RESPONSES as u8;
        let user_min = data.min_ack_responses;
        let chain_min = match self
            .blockchain_manager
            .get_minimum_required_signatures(blockchain)
            .await
        {
            Ok(min) => min as u8,
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    blockchain = %blockchain,
                    error = %e,
                    "Failed to fetch on-chain minimumRequiredSignatures, using default/user value"
                );
                0
            }
        };

        let min_ack_responses = default_min.max(chain_min).max(user_min);

        let shard_nodes = match self
            .repository_manager
            .shard_repository()
            .get_all_peer_records(blockchain.as_str())
            .await
        {
            Ok(shard_nodes) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    shard_nodes_count = shard_nodes.len(),
                    "Retrieved shard nodes from repository"
                );
                shard_nodes
            }
            Err(e) => {
                let error_message = format!(
                    "Failed to get shard nodes from repository for operation: {operation_id}. Error: {e}"
                );
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to get shard nodes"
                );

                self.publish_store_operation_status_service
                    .mark_failed(operation_id, error_message)
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

        let my_peer_id = *self.network_manager.peer_id();

        // Check if we are in the shard nodes (publisher node)
        let self_in_shard = shard_nodes
            .iter()
            .any(|node| node.peer_id.parse::<PeerId>().ok() == Some(my_peer_id));

        // Parse peer IDs and filter out self
        let remote_peers: Vec<PeerId> = shard_nodes
            .iter()
            .filter_map(|record| record.peer_id.parse().ok())
            .filter(|peer_id| *peer_id != my_peer_id)
            .collect();

        // Total peers includes self if we're in the shard
        let total_peers = if self_in_shard {
            remote_peers.len() as u16 + 1
        } else {
            remote_peers.len() as u16
        };
        tracing::Span::current().record("peer_count", tracing::field::display(total_peers));

        tracing::debug!(
            operation_id = %operation_id,
            total_peers = total_peers,
            remote_peers = remote_peers.len(),
            self_in_shard = self_in_shard,
            my_peer_id = %my_peer_id,
            "Parsed peer IDs from shard nodes"
        );

        let min_ack_required = min_ack_responses as u16;
        let min_required_peers = protocols::publish_store::MIN_PEERS.max(min_ack_required as usize);
        if (total_peers as usize) < min_required_peers {
            let error_message = format!(
                "Unable to find enough nodes for operation: {operation_id}. Minimum number of nodes required: {min_required_peers}"
            );

            self.publish_store_operation_status_service
                .mark_failed(operation_id, error_message)
                .await;

            return CommandExecutionResult::Completed;
        }

        let identity_id = self.blockchain_manager.identity_id(blockchain);

        let Some(dataset_root_hex) = dataset_root.strip_prefix("0x") else {
            self.publish_store_operation_status_service
                .mark_failed(operation_id, "Dataset root missing '0x' prefix".to_string())
                .await;
            return CommandExecutionResult::Completed;
        };

        // Create and store publisher signature directly to result storage
        match self
            .create_publisher_signature(blockchain, dataset_root, identity_id)
            .await
        {
            Ok(sig) => {
                // Store publisher signature to result storage immediately
                if let Err(e) = self.publish_store_operation_status_service.update_result(
                    operation_id,
                    PublishStoreOperationResult::new(None, Vec::new()),
                    |result| {
                        result.publisher_signature = Some(sig);
                    },
                ) {
                    tracing::warn!(
                        operation_id = %operation_id,
                        error = %e,
                        "Failed to store publisher signature to result storage"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to create publisher signature, operation may still succeed with network signatures"
                );
            }
        }

        if let Err(e) = self.pending_storage_service.store_dataset(
            operation_id,
            dataset_root,
            dataset,
            &my_peer_id.to_base58(),
        ) {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to store dataset in pending storage"
            );
            self.publish_store_operation_status_service
                .mark_failed(operation_id, format!("Failed to store dataset: {}", e))
                .await;
            return CommandExecutionResult::Completed;
        }

        // Handle self-node signature if we're in the shard
        if self_in_shard {
            tracing::debug!(
                operation_id = %operation_id,
                peer = %my_peer_id,
                "Processing self-node (publisher), handling signature locally"
            );
            if let Err(e) = self
                .handle_self_node_signature(operation_id, blockchain, dataset_root_hex, identity_id)
                .await
            {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to handle self-node signature, continuing with other nodes"
                );
            } else {
                tracing::debug!(
                    operation_id = %operation_id,
                    "Self-node signature handled successfully"
                );
            }
        }

        // Build the store request data
        let store_request_data = StoreRequestData::new(
            dataset.public.clone(),
            dataset_root.clone(),
            blockchain.to_owned(),
        );

        // Send requests to peers with bounded concurrency and process responses as they arrive.
        let mut success_count: u16 = if self_in_shard { 1 } else { 0 }; // Include self-node if in shard
        let mut failure_count: u16 = 0;
        let mut reached_threshold = success_count >= min_ack_required;

        if !reached_threshold && !remote_peers.is_empty() {
            let mut futures = FuturesUnordered::new();
            let mut peers_iter = remote_peers.iter().cloned();
            let limit = protocols::publish_store::CONCURRENT_PEERS
                .max(1)
                .min(remote_peers.len());

            for _ in 0..limit {
                if let Some(peer) = peers_iter.next() {
                    futures.push(send_store_request_to_peer(
                        self,
                        peer,
                        operation_id,
                        store_request_data.clone(),
                    ));
                }
            }

            while let Some((peer, result)) = futures.next().await {
                match result {
                    Ok(response) => {
                        let is_valid = self.process_store_response(operation_id, &peer, &response);

                        if !is_valid {
                            failure_count += 1;
                        } else {
                            success_count += 1;
                        }
                    }
                    Err(e) => {
                        failure_count += 1;
                        tracing::debug!(
                            operation_id = %operation_id,
                            peer = %peer,
                            error = %e,
                            "Store request failed"
                        );
                    }
                }

                if success_count >= min_ack_required {
                    reached_threshold = true;
                    break;
                }

                if let Some(peer) = peers_iter.next() {
                    futures.push(send_store_request_to_peer(
                        self,
                        peer,
                        operation_id,
                        store_request_data.clone(),
                    ));
                }
            }
        }

        if reached_threshold {
            tracing::info!(
                operation_id = %operation_id,
                success_count = success_count,
                failure_count = failure_count,
                "Publish store completed"
            );

            if let Err(e) = self
                .publish_store_operation_status_service
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
            "Failed to get enough signatures. Success: {}, Failed: {}, Required: {}",
            success_count, failure_count, min_ack_responses
        );
        tracing::warn!(operation_id = %operation_id, %error_message, "Publish store failed");
        self.publish_store_operation_status_service
            .mark_failed(operation_id, error_message)
            .await;

        CommandExecutionResult::Completed
    }
}

async fn send_store_request_to_peer(
    handler: &SendPublishStoreRequestsCommandHandler,
    peer: PeerId,
    operation_id: Uuid,
    request_data: StoreRequestData,
) -> (PeerId, Result<StoreResponseData, NetworkError>) {
    let result = handler
        .network_manager
        .send_store_request(peer, operation_id, request_data)
        .await;
    (peer, result)
}
