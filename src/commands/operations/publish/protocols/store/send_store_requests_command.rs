use std::sync::Arc;

use blockchain::{BlockchainId, BlockchainManager, H256, utils::keccak256_encode_packed};
use libp2p::PeerId;
use network::NetworkManager;
use repository::RepositoryManager;
use triple_store::Assertion;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    controllers::rpc_controller::{NetworkProtocols, messages::StoreRequestData},
    operations::{PublishOperation, PublishOperationResult, SignatureData},
    services::{
        BatchSender, pending_storage_service::PendingStorageService,
        operation::OperationService as GenericOperationService,
    },
};

/// Command data for sending publish requests to network nodes.
/// Dataset is passed inline instead of being retrieved from storage.
#[derive(Clone)]
pub struct SendStoreRequestsCommandData {
    pub operation_id: Uuid,
    pub blockchain: BlockchainId,
    pub dataset_root: String,
    /// User-provided minimum ACK responses. If None, uses max(default, chain_min).
    pub min_ack_responses: Option<u8>,
    pub dataset: Assertion,
}

impl SendStoreRequestsCommandData {
    pub fn new(
        operation_id: Uuid,
        blockchain: BlockchainId,
        dataset_root: String,
        min_ack_responses: Option<u8>,
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

pub struct SendStoreRequestsCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    blockchain_manager: Arc<BlockchainManager>,
    publish_operation_service: Arc<GenericOperationService<PublishOperation>>,
    pending_storage_service: Arc<PendingStorageService>,
}

impl SendStoreRequestsCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
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
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

        // Record as a network response
        self.publish_operation_service
            .record_response(operation_id, true)
            .await?;

        Ok(())
    }

    /// Create publisher signature and store it in context.
    /// Returns the SignatureData for storage in context.
    async fn create_publisher_signature(
        &self,
        blockchain: &BlockchainId,
        dataset_root: &str,
        identity_id: u128,
    ) -> Result<SignatureData, Box<dyn std::error::Error + Send + Sync>> {
        let dataset_root_h256: H256 = dataset_root.parse()?;
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
                &format!("0x{}", blockchain::utils::to_hex_string(message_hash)),
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
}

impl CommandHandler<SendStoreRequestsCommandData> for SendStoreRequestsCommandHandler {
    async fn execute(&self, data: &SendStoreRequestsCommandData) -> CommandExecutionResult {
        use crate::services::operation::Operation;

        let operation_id = data.operation_id;
        let blockchain = &data.blockchain;
        let dataset_root = &data.dataset_root;
        let dataset = &data.dataset;

        // Determine effective min_ack_responses using max(default, chain_min, user_provided)
        let default_min = PublishOperation::MIN_ACK_RESPONSES as u8;
        let user_min = data.min_ack_responses.unwrap_or(0);

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
            .get_all_peer_records(blockchain.as_str(), true)
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

        // Initialize progress tracking and get completion receiver
        let completion_rx = match self
            .publish_operation_service
            .initialize_progress(operation_id, total_peers, min_ack_responses as u16)
            .await
        {
            Ok(rx) => rx,
            Err(e) => {
                self.publish_operation_service
                    .mark_failed(operation_id, e.to_string())
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

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

        let dataset_root_hex = match dataset_root.strip_prefix("0x") {
            Some(hex) => hex,
            None => {
                self.publish_operation_service
                    .mark_failed(operation_id, "Dataset root missing '0x' prefix".to_string())
                    .await;
                return CommandExecutionResult::Completed;
            }
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

        if let Err(e) = self
            .pending_storage_service
            .store_dataset(operation_id, dataset_root, dataset, &my_peer_id.to_base58())
        {
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

        // Create batch sender and send requests to remote peers
        let batch_sender = BatchSender::<PublishOperation>::from_operation();

        let result = batch_sender
            .send_batched_until_completion(
                operation_id,
                remote_peers,
                store_request_data,
                Arc::clone(&self.network_manager),
                Arc::clone(self.publish_operation_service.request_tracker()),
                completion_rx,
            )
            .await;

        tracing::info!(
            operation_id = %operation_id,
            sent = result.sent_count,
            failed = result.failed_count,
            early_completion = result.early_completion,
            "Publish requests batch sending completed"
        );

        CommandExecutionResult::Completed
    }
}
