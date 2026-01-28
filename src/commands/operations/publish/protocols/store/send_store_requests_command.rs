use std::sync::Arc;

use futures::future::join_all;
use libp2p::PeerId;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    error::NodeError,
    managers::{
        blockchain::{BlockchainId, BlockchainManager, H256, utils::keccak256_encode_packed},
        network::{
            NetworkManager,
            messages::{StoreRequestData, StoreResponseData},
        },
        repository::RepositoryManager,
        triple_store::Assertion,
    },
    operations::{PublishOperation, PublishOperationResult, SignatureData},
    services::{
        operation::{Operation, OperationService as GenericOperationService},
        pending_storage_service::PendingStorageService,
    },
};

/// Command data for sending publish requests to network nodes.
/// Dataset is passed inline instead of being retrieved from storage.
#[derive(Clone)]
pub(crate) struct SendStoreRequestsCommandData {
    pub operation_id: Uuid,
    pub blockchain: BlockchainId,
    pub dataset_root: String,
    pub min_ack_responses: u8,
    pub dataset: Assertion,
}

impl SendStoreRequestsCommandData {
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

pub(crate) struct SendStoreRequestsCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager>,
    blockchain_manager: Arc<BlockchainManager>,
    publish_operation_service: Arc<GenericOperationService<PublishOperation>>,
    pending_storage_service: Arc<PendingStorageService>,
}

impl SendStoreRequestsCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            publish_operation_service: Arc::clone(context.publish_operation_service()),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
        }
    }

    /// Handle self-node signature (when publisher is in the shard).
    /// Stores the signature directly to redb and records a successful response.
    async fn handle_self_node_signature(
        &self,
        operation_id: Uuid,
        blockchain: &BlockchainId,
        dataset_root_hex: &str,
        identity_id: u128,
    ) -> Result<(), NodeError> {
        let signature = self
            .blockchain_manager
            .sign_message(blockchain, dataset_root_hex)
            .await?;

        // Add self-node signature directly to redb as a network signature
        let sig_data = SignatureData::new(
            identity_id.to_string(),
            signature.v,
            signature.r.clone(),
            signature.s.clone(),
            signature.vs.clone(),
        );

        self.publish_operation_service.update_result(
            operation_id,
            PublishOperationResult::new(None, Vec::new()),
            |result| {
                result.network_signatures.push(sig_data);
            },
        )?;

        Ok(())
    }

    /// Create publisher signature and store it in context.
    /// Returns the SignatureData for storage in context.
    async fn create_publisher_signature(
        &self,
        blockchain: &BlockchainId,
        dataset_root: &str,
        identity_id: u128,
    ) -> Result<SignatureData, NodeError> {
        let dataset_root_h256: H256 = dataset_root
            .parse()
            .map_err(|e| NodeError::Other(format!("Invalid dataset root hex: {e}")))?;
        // JS uses: keccak256EncodePacked(['uint72', 'bytes32'], [identityId, datasetRoot])
        // uint72 = 9 bytes, so we encode identity_id as FixedBytes(9) to match Solidity's packed
        // encoding
        let identity_bytes = {
            let bytes = identity_id.to_be_bytes(); // u128 = 16 bytes
            let mut out = [0u8; 9];
            out.copy_from_slice(&bytes[16 - 9..]); // Take last 9 bytes for uint72
            out
        };
        let message_hash =
            keccak256_encode_packed(&[&identity_bytes, dataset_root_h256.as_slice()]);
        let signature = self
            .blockchain_manager
            .sign_message(
                blockchain,
                &format!(
                    "0x{}",
                    crate::managers::blockchain::utils::to_hex_string(message_hash)
                ),
            )
            .await?;

        Ok(SignatureData::new(
            identity_id.to_string(),
            signature.v,
            signature.r,
            signature.s,
            signature.vs,
        ))
    }

    /// Process a store response and store the signature if valid.
    ///
    /// Returns true if the response is a valid ACK with signature data.
    fn process_store_response(
        &self,
        operation_id: Uuid,
        peer: &PeerId,
        response: &StoreResponseData,
    ) -> bool {
        match response {
            StoreResponseData::Data {
                identity_id,
                signature,
            } => {
                let sig_data = SignatureData::new(
                    identity_id.to_string(),
                    signature.v,
                    signature.r.clone(),
                    signature.s.clone(),
                    signature.vs.clone(),
                );

                // Store signature incrementally to redb
                if let Err(e) = self.publish_operation_service.update_result(
                    operation_id,
                    PublishOperationResult::new(None, Vec::new()),
                    |result| {
                        result.network_signatures.push(sig_data);
                    },
                ) {
                    tracing::error!(
                        operation_id = %operation_id,
                        peer = %peer,
                        error = %e,
                        "Failed to store network signature"
                    );
                    return false;
                }

                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    identity_id = %identity_id,
                    "Signature stored successfully"
                );
                true
            }
            StoreResponseData::Error { error_message } => {
                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %peer,
                    error = %error_message,
                    "Peer returned error response"
                );
                false
            }
        }
    }
}

impl CommandHandler<SendStoreRequestsCommandData> for SendStoreRequestsCommandHandler {
    async fn execute(&self, data: &SendStoreRequestsCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;
        let blockchain = &data.blockchain;
        let dataset_root = &data.dataset_root;
        let dataset = &data.dataset;

        // Determine effective min_ack_responses using max(default, chain_min, user_provided)
        let default_min = PublishOperation::MIN_ACK_RESPONSES as u8;
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

        tracing::info!(
            operation_id = %operation_id,
            blockchain = %blockchain,
            dataset_root = %dataset_root,
            default_min = default_min,
            chain_min = chain_min,
            user_min = user_min,
            effective_min_ack = min_ack_responses,
            "Starting SendPublishRequests command"
        );

        let shard_nodes = match self
            .repository_manager
            .shard_repository()
            .get_all_peer_records(blockchain.as_str())
            .await
        {
            Ok(shard_nodes) => {
                tracing::info!(
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

                self.publish_operation_service
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

        tracing::info!(
            operation_id = %operation_id,
            total_peers = total_peers,
            remote_peers = remote_peers.len(),
            self_in_shard = self_in_shard,
            my_peer_id = %my_peer_id,
            "Parsed peer IDs from shard nodes"
        );

        if (total_peers as usize) < min_ack_responses as usize {
            let error_message = format!(
                "Unable to find enough nodes for operation: {operation_id}. Minimum number of nodes required: {min_ack_responses}"
            );

            self.publish_operation_service
                .mark_failed(operation_id, error_message)
                .await;

            return CommandExecutionResult::Completed;
        }

        let identity_id = match self.blockchain_manager.get_identity_id(blockchain).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                self.publish_operation_service
                    .mark_failed(
                        operation_id,
                        format!("Identity ID not found for blockchain {}", blockchain),
                    )
                    .await;
                return CommandExecutionResult::Completed;
            }
            Err(e) => {
                self.publish_operation_service
                    .mark_failed(operation_id, format!("Failed to get identity ID: {}", e))
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

        let Some(dataset_root_hex) = dataset_root.strip_prefix("0x") else {
            self.publish_operation_service
                .mark_failed(operation_id, "Dataset root missing '0x' prefix".to_string())
                .await;
            return CommandExecutionResult::Completed;
        };

        // Create and store publisher signature directly to redb
        match self
            .create_publisher_signature(blockchain, dataset_root, identity_id)
            .await
        {
            Ok(sig) => {
                // Store publisher signature to redb immediately
                if let Err(e) = self.publish_operation_service.update_result(
                    operation_id,
                    PublishOperationResult::new(None, Vec::new()),
                    |result| {
                        result.publisher_signature = Some(sig);
                    },
                ) {
                    tracing::warn!(
                        operation_id = %operation_id,
                        error = %e,
                        "Failed to store publisher signature to redb"
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
            self.publish_operation_service
                .mark_failed(operation_id, format!("Failed to store dataset: {}", e))
                .await;
            return CommandExecutionResult::Completed;
        }

        // Handle self-node signature if we're in the shard
        if self_in_shard {
            tracing::info!(
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
                tracing::info!(
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

        // Send requests to peers in chunks and process responses directly
        let mut success_count: u16 = if self_in_shard { 1 } else { 0 }; // Include self-node if in shard
        let mut failure_count: u16 = 0;

        for (chunk_idx, peer_chunk) in remote_peers
            .chunks(PublishOperation::CONCURRENT_PEERS)
            .enumerate()
        {
            // Check if we've already met the threshold (e.g., from self-node)
            if success_count >= min_ack_responses as u16 {
                tracing::info!(
                    operation_id = %operation_id,
                    success_count = success_count,
                    min_required = min_ack_responses,
                    "Success threshold already reached, skipping remaining peer chunks"
                );
                break;
            }

            tracing::debug!(
                operation_id = %operation_id,
                chunk = chunk_idx,
                peer_count = peer_chunk.len(),
                "Sending store requests to peer chunk"
            );

            // Send all requests in this chunk concurrently
            let request_futures: Vec<_> = peer_chunk
                .iter()
                .map(|peer| {
                    let peer = *peer;
                    let request_data = store_request_data.clone();
                    let network_manager = Arc::clone(&self.network_manager);
                    async move {
                        // Get peer addresses from Kademlia for reliable request delivery
                        let addresses = network_manager
                            .get_peer_addresses(peer)
                            .await
                            .unwrap_or_default();
                        let result = network_manager
                            .send_store_request(peer, addresses, operation_id, request_data)
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
                        let is_valid = self.process_store_response(operation_id, &peer, &response);

                        if !is_valid {
                            failure_count += 1;
                            continue;
                        }

                        success_count += 1;

                        // Check if we've met the success threshold
                        if success_count >= min_ack_responses as u16 {
                            tracing::info!(
                                operation_id = %operation_id,
                                success_count = success_count,
                                failure_count = failure_count,
                                chunk = chunk_idx,
                                "Publish operation completed - success threshold reached"
                            );

                            // Mark operation as completed with final counts
                            if let Err(e) = self
                                .publish_operation_service
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
            "Failed to get enough signatures. Success: {}, Failed: {}, Required: {}",
            success_count, failure_count, min_ack_responses
        );
        tracing::warn!(operation_id = %operation_id, %error_message);
        self.publish_operation_service
            .mark_failed(operation_id, error_message)
            .await;

        CommandExecutionResult::Completed
    }
}
