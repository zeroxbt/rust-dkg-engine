use std::sync::Arc;

use blockchain::{BlockchainId, BlockchainManager, H256, utils::keccak256_encode_packed};
use futures::future::join_all;
use libp2p::PeerId;
use network::{
    NetworkManager, RequestMessage,
    message::{RequestMessageHeader, RequestMessageType},
};
use repository::RepositoryManager;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    network::{NetworkProtocols, ProtocolRequest},
    services::operation_manager::OperationManager,
    types::{models::Assertion, protocol::StoreRequestData},
};

/// Command data for sending publish requests to network nodes.
/// Dataset is passed inline instead of being retrieved from storage.
#[derive(Clone)]
pub struct SendPublishRequestsCommandData {
    pub operation_id: Uuid,
    pub blockchain: BlockchainId,
    pub dataset_root: String,
    pub min_ack_responses: u8,
    pub dataset: Assertion,
}

impl SendPublishRequestsCommandData {
    pub fn new(
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

pub struct SendPublishRequestsCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    blockchain_manager: Arc<BlockchainManager>,
    publish_operation_manager: Arc<OperationManager>,
}

impl SendPublishRequestsCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            publish_operation_manager: Arc::clone(context.publish_operation_manager()),
        }
    }

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
        let signature = blockchain::utils::split_signature(signature)?;

        self.repository_manager
            .signature_repository()
            .store_network_signature(
                operation_id,
                &identity_id.to_string(),
                signature.v,
                &signature.r,
                &signature.s,
                &signature.vs,
            )
            .await?;

        // treat as network response
        self.publish_operation_manager
            .record_response(operation_id, true)
            .await?;

        Ok(())
    }

    async fn store_publisher_signature(
        &self,
        operation_id: Uuid,
        blockchain: &BlockchainId,
        dataset_root: &str,
        identity_id: u128,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
        let signature = blockchain::utils::split_signature(signature)?;

        self.repository_manager
            .signature_repository()
            .store_publisher_signature(
                operation_id,
                &identity_id.to_string(),
                signature.v,
                &signature.r,
                &signature.s,
                &signature.vs,
            )
            .await?;

        Ok(())
    }
}

impl CommandHandler<SendPublishRequestsCommandData> for SendPublishRequestsCommandHandler {
    async fn execute(&self, data: &SendPublishRequestsCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;
        let blockchain = &data.blockchain;
        let dataset_root = &data.dataset_root;
        let min_ack_responses = data.min_ack_responses;
        let dataset = &data.dataset;

        tracing::info!(
            operation_id = %operation_id,
            blockchain = %blockchain,
            dataset_root = %dataset_root,
            min_ack_responses = %min_ack_responses,
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

                self.publish_operation_manager
                    .mark_failed(operation_id, error_message)
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

        let peers: Vec<PeerId> = shard_nodes
            .iter()
            .filter_map(|record| record.peer_id.parse().ok())
            .collect();

        let total_peers = peers.len() as u16;

        tracing::info!(
            operation_id = %operation_id,
            total_peers = total_peers,
            peer_ids = ?peers.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
            "Parsed peer IDs from shard nodes"
        );

        // Initialize progress tracking (operation record already created in handle_request)
        if let Err(e) = self
            .publish_operation_manager
            .initialize_progress(operation_id, total_peers, min_ack_responses as u16)
            .await
        {
            self.publish_operation_manager
                .mark_failed(operation_id, e.to_string())
                .await;
            return CommandExecutionResult::Completed;
        }

        if peers.len() < min_ack_responses as usize {
            let error_message = format!(
                "Unable to find enough nodes for operation: {operation_id}. Minimum number of nodes required: {min_ack_responses}"
            );

            self.publish_operation_manager
                .mark_failed(operation_id, error_message)
                .await;

            return CommandExecutionResult::Completed;
        }

        let my_peer_id = *self.network_manager.peer_id();
        let mut send_futures = Vec::with_capacity(shard_nodes.len());

        tracing::info!(
            operation_id = %operation_id,
            my_peer_id = %my_peer_id,
            "Self peer ID identified"
        );

        let identity_id = match self.blockchain_manager.get_identity_id(blockchain).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                self.publish_operation_manager
                    .mark_failed(
                        operation_id,
                        format!("Identity ID not found for blockchain {}", blockchain),
                    )
                    .await;
                return CommandExecutionResult::Completed;
            }
            Err(e) => {
                self.publish_operation_manager
                    .mark_failed(operation_id, format!("Failed to get identity ID: {}", e))
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

        let dataset_root_hex = match dataset_root.strip_prefix("0x") {
            Some(hex) => hex,
            None => {
                self.publish_operation_manager
                    .mark_failed(operation_id, "Dataset root missing '0x' prefix".to_string())
                    .await;
                return CommandExecutionResult::Completed;
            }
        };

        for node in shard_nodes {
            let remote_peer_id: PeerId = match node.peer_id.parse() {
                Ok(id) => id,
                Err(_) => continue,
            };

            if remote_peer_id == my_peer_id {
                tracing::info!(
                    operation_id = %operation_id,
                    peer = %remote_peer_id,
                    "Processing self-node (publisher), handling signature locally"
                );
                if let Err(e) = self
                    .handle_self_node_signature(
                        operation_id,
                        blockchain,
                        dataset_root_hex,
                        identity_id,
                    )
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
            } else {
                tracing::info!(
                    operation_id = %operation_id,
                    remote_peer = %remote_peer_id,
                    "Preparing to send store request to remote peer"
                );
                let network_manager = Arc::clone(&self.network_manager);
                let message = RequestMessage {
                    header: RequestMessageHeader {
                        operation_id,
                        message_type: RequestMessageType::ProtocolRequest,
                    },
                    data: StoreRequestData::new(
                        dataset.public.clone(),
                        dataset_root.clone(),
                        blockchain.to_owned(),
                    ),
                };

                let op_id = operation_id;
                send_futures.push(async move {
                    tracing::debug!(
                        operation_id = %op_id,
                        peer = %remote_peer_id,
                        "Sending store request to peer"
                    );
                    match network_manager
                        .send_protocol_request(ProtocolRequest::Store {
                            peer: remote_peer_id,
                            message,
                        })
                        .await
                    {
                        Ok(_) => {
                            tracing::info!(
                                operation_id = %op_id,
                                peer = %remote_peer_id,
                                "Store request sent successfully to peer"
                            );
                        }
                        Err(e) => {
                            tracing::error!(
                                operation_id = %op_id,
                                peer = %remote_peer_id,
                                error = %e,
                                "Failed to send store request"
                            );
                        }
                    }
                });
            }
        }

        tracing::info!(
            operation_id = %operation_id,
            futures_count = send_futures.len(),
            "Waiting for all store requests to complete"
        );

        join_all(send_futures).await;

        tracing::info!(
            operation_id = %operation_id,
            "All store requests have been sent"
        );

        // Store publisher signature
        if let Err(e) = self
            .store_publisher_signature(operation_id, blockchain, dataset_root, identity_id)
            .await
        {
            tracing::warn!(
                operation_id = %operation_id,
                error = %e,
                "Failed to store publisher signature, operation may still succeed with network signatures"
            );
        }

        CommandExecutionResult::Completed
    }
}
