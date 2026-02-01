use std::sync::Arc;

use libp2p::PeerId;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::{BlockchainId, BlockchainManager},
        network::{NetworkManager, messages::StoreAck},
        repository::RepositoryManager,
        triple_store::Assertion,
    },
    services::{ResponseChannels, pending_storage_service::PendingStorageService},
    utils::validation,
};

/// Command data for handling incoming publish store requests.
/// Dataset is passed inline; channel is retrieved from session manager.
#[derive(Clone)]
pub(crate) struct HandlePublishStoreRequestCommandData {
    pub blockchain: BlockchainId,
    pub operation_id: Uuid,
    pub dataset_root: String,
    pub remote_peer_id: PeerId,
    pub dataset: Assertion,
}

impl HandlePublishStoreRequestCommandData {
    pub(crate) fn new(
        blockchain: BlockchainId,
        operation_id: Uuid,
        dataset_root: String,
        remote_peer_id: PeerId,
        dataset: Assertion,
    ) -> Self {
        Self {
            blockchain,
            operation_id,
            dataset_root,
            remote_peer_id,
            dataset,
        }
    }
}

pub(crate) struct HandlePublishStoreRequestCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    pub(super) network_manager: Arc<NetworkManager>,
    blockchain_manager: Arc<BlockchainManager>,
    response_channels: Arc<ResponseChannels<StoreAck>>,
    pending_storage_service: Arc<PendingStorageService>,
}

impl HandlePublishStoreRequestCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            response_channels: Arc::clone(context.store_response_channels()),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
        }
    }
}

impl CommandHandler<HandlePublishStoreRequestCommandData>
    for HandlePublishStoreRequestCommandHandler
{
    #[instrument(
        name = "op.publish_store.recv",
        skip(self, data),
        fields(
            operation_id = %data.operation_id,
            protocol = "publish_store",
            direction = "recv",
            blockchain = %data.blockchain,
            dataset_root = %data.dataset_root,
            remote_peer = %data.remote_peer_id,
        )
    )]
    async fn execute(&self, data: &HandlePublishStoreRequestCommandData) -> CommandExecutionResult {
        let operation_id = data.operation_id;
        let blockchain = &data.blockchain;
        let dataset_root = &data.dataset_root;
        let remote_peer_id = &data.remote_peer_id;
        let dataset = &data.dataset;

        // Retrieve the response channel
        let Some(channel) = self
            .response_channels
            .retrieve(remote_peer_id, operation_id)
        else {
            tracing::error!(
                operation_id = %operation_id,
                peer = %remote_peer_id,
                "No cached response channel found. Channel may have expired."
            );
            return CommandExecutionResult::Completed;
        };
        match self
            .repository_manager
            .shard_repository()
            .get_peer_record(blockchain.as_str(), &remote_peer_id.to_base58())
            .await
        {
            Ok(Some(_record)) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    remote_peer_id = %remote_peer_id,
                    "Remote peer validated against shard repository"
                );
            }
            invalid_result => {
                let error_message = match &invalid_result {
                    Err(e) => format!(
                        "Failed to get remote peer: {remote_peer_id} in shard_repository for operation: {operation_id}. Error: {e}"
                    ),
                    Ok(None) => format!(
                        "Remote peer {} not found in shard repository for blockchain: {}, operation: {operation_id}",
                        remote_peer_id,
                        blockchain.as_str()
                    ),
                    _ => format!(
                        "Invalid shard on blockchain: {}, operation: {operation_id}",
                        blockchain.as_str()
                    ),
                };

                tracing::warn!(
                    operation_id = %operation_id,
                    remote_peer_id = %remote_peer_id,
                    blockchain = %blockchain,
                    error = %error_message,
                    "Peer validation failed - sending NACK"
                );

                self.send_nack(channel, operation_id, "Invalid neighbourhood")
                    .await;

                return CommandExecutionResult::Completed;
            }
        };

        let computed_dataset_root = validation::calculate_merkle_root(&dataset.public);

        tracing::debug!(
            operation_id = %operation_id,
            received_dataset_root = %dataset_root,
            computed_dataset_root = %computed_dataset_root,
            roots_match = (*dataset_root == computed_dataset_root),
            "Dataset root validation"
        );

        if *dataset_root != computed_dataset_root {
            tracing::warn!(
                operation_id = %operation_id,
                received = %dataset_root,
                computed = %computed_dataset_root,
                "Dataset root mismatch - sending NACK"
            );
            self.send_nack(
                channel,
                operation_id,
                &format!(
                    "Dataset root validation failed. Received dataset root: {}; Calculated dataset root: {}",
                    dataset_root, computed_dataset_root
                ),
            )
            .await;

            return CommandExecutionResult::Completed;
        }

        let identity_id = self.blockchain_manager.identity_id(blockchain);
        tracing::debug!(
            operation_id = %operation_id,
            identity_id = %identity_id,
            "Identity ID resolved"
        );

        let Some(dataset_root_hex) = dataset_root.strip_prefix("0x") else {
            tracing::warn!(
            operation_id = %operation_id,
            dataset_root = %dataset_root,
            "Dataset root missing '0x' prefix - sending NACK"
            );
            self.send_nack(channel, operation_id, "Dataset root missing '0x' prefix")
                .await;

            return CommandExecutionResult::Completed;
        };

        let signature = match self
            .blockchain_manager
            .sign_message(blockchain, dataset_root_hex)
            .await
        {
            Ok(sig) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    "Message signed successfully"
                );
                sig
            }
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to sign message - sending NACK"
                );
                self.send_nack(
                    channel,
                    operation_id,
                    &format!("Failed to sign message: {}", e),
                )
                .await;

                return CommandExecutionResult::Completed;
            }
        };

        if let Err(e) = self.pending_storage_service.store_dataset(
            operation_id,
            dataset_root,
            dataset,
            &remote_peer_id.to_base58(),
        ) {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to store dataset in pending storage - sending NACK"
            );
            self.send_nack(
                channel,
                operation_id,
                &format!("Failed to store dataset: {}", e),
            )
            .await;
            return CommandExecutionResult::Completed;
        }

        tracing::debug!(
            operation_id = %operation_id,
            peer = %remote_peer_id,
            identity_id = %identity_id,
            "Sending ACK response with signature"
        );

        self.send_ack(channel, operation_id, identity_id, signature)
            .await;

        tracing::debug!(
            operation_id = %operation_id,
            peer = %remote_peer_id,
            "Store request validated; ACK sent"
        );

        CommandExecutionResult::Completed
    }
}
