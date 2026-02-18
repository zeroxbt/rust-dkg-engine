use std::sync::Arc;

use dkg_blockchain::{BlockchainId, BlockchainManager};
use dkg_domain::{Assertion, SignatureComponents};
use dkg_key_value_store::{PublishTmpDataset, PublishTmpDatasetStore};
use dkg_network::{NetworkManager, PeerId, ResponseHandle, StoreAck};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    commands::HandlePublishStoreRequestDeps,
    commands::{executor::CommandOutcome, registry::CommandHandler},
    runtime_state::PeerDirectory,
    state::ResponseChannels,
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
    pub(super) network_manager: Arc<NetworkManager>,
    blockchain_manager: Arc<BlockchainManager>,
    peer_directory: Arc<PeerDirectory>,
    response_channels: Arc<ResponseChannels<StoreAck>>,
    publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
}

impl HandlePublishStoreRequestCommandHandler {
    pub(crate) fn new(deps: HandlePublishStoreRequestDeps) -> Self {
        Self {
            network_manager: deps.network_manager,
            blockchain_manager: deps.blockchain_manager,
            peer_directory: deps.peer_directory,
            response_channels: deps.store_response_channels,
            publish_tmp_dataset_store: deps.publish_tmp_dataset_store,
        }
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseHandle<StoreAck>,
        operation_id: Uuid,
        error_message: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_store_nack(channel, operation_id, error_message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send store NACK response"
            );
        }
    }

    pub(crate) async fn send_ack(
        &self,
        channel: ResponseHandle<StoreAck>,
        operation_id: Uuid,
        identity_id: u128,
        signature: SignatureComponents,
    ) {
        if let Err(e) = self
            .network_manager
            .send_store_ack(
                channel,
                operation_id,
                StoreAck {
                    identity_id,
                    signature,
                },
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send store ACK response"
            );
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
    async fn execute(&self, data: &HandlePublishStoreRequestCommandData) -> CommandOutcome {
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
            return CommandOutcome::Completed;
        };

        // Validate that *this node* is in the shard for the given blockchain.
        // (The sender being in shard is not the relevant check here.)
        let local_peer_id = self.network_manager.peer_id();
        if self
            .peer_directory
            .is_peer_in_shard(blockchain, local_peer_id)
        {
            tracing::debug!(
                operation_id = %operation_id,
                local_peer_id = %local_peer_id,
                blockchain = %blockchain,
                "Local node validated against shard membership"
            );
        } else {
            tracing::warn!(
                operation_id = %operation_id,
                local_peer_id = %local_peer_id,
                blockchain = %blockchain,
                "Local node not found in shard - sending NACK"
            );

            self.send_nack(channel, operation_id, "Local node not in shard")
                .await;

            return CommandOutcome::Completed;
        }

        let computed_dataset_root = dkg_domain::calculate_merkle_root(&dataset.public);

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

            return CommandOutcome::Completed;
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

            return CommandOutcome::Completed;
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

                return CommandOutcome::Completed;
            }
        };

        let pending = PublishTmpDataset::new(
            dataset_root.to_owned(),
            dataset.clone(),
            remote_peer_id.to_base58(),
        );
        if let Err(e) = self
            .publish_tmp_dataset_store
            .store(operation_id, pending)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to store dataset in publish tmp dataset store - sending NACK"
            );
            self.send_nack(
                channel,
                operation_id,
                &format!("Failed to store dataset: {}", e),
            )
            .await;
            return CommandOutcome::Completed;
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

        CommandOutcome::Completed
    }
}
