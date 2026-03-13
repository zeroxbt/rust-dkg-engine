use std::sync::Arc;

use dkg_blockchain::{BlockchainId, BlockchainManager};
use dkg_domain::{Assertion, SignatureComponents};
use dkg_key_value_store::{PublishTmpDataset, PublishTmpDatasetStore};
use dkg_network::{NetworkManager, PeerId, ResponseHandle, StoreAck, StoreRequestData};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    application::signature,
    commands::{HandlePublishStoreRequestDeps, executor::CommandOutcome, registry::CommandHandler},
    peer_registry::PeerRegistry,
};

/// Command data for handling incoming publish store requests.
/// Dataset is passed inline; channel is retrieved from session manager.
pub(crate) struct HandlePublishStoreRequestCommandData {
    pub operation_id: Uuid,
    pub request: StoreRequestData,
    pub remote_peer_id: PeerId,
    pub response: ResponseHandle<StoreAck>,
}

impl HandlePublishStoreRequestCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        request: StoreRequestData,
        remote_peer_id: PeerId,
        response: ResponseHandle<StoreAck>,
    ) -> Self {
        Self {
            operation_id,
            request,
            remote_peer_id,
            response,
        }
    }
}

pub(crate) struct HandlePublishStoreRequestCommandHandler {
    pub(super) network_manager: Arc<NetworkManager>,
    blockchain_manager: Arc<BlockchainManager>,
    peer_registry: Arc<PeerRegistry>,
    publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
}

impl HandlePublishStoreRequestCommandHandler {
    pub(crate) fn new(deps: HandlePublishStoreRequestDeps) -> Self {
        Self {
            network_manager: deps.network_manager,
            blockchain_manager: deps.blockchain_manager,
            peer_registry: deps.peer_registry,
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
            blockchain = tracing::field::Empty,
            dataset_root = tracing::field::Empty,
            remote_peer = %data.remote_peer_id,
        )
    )]
    async fn execute(&self, data: HandlePublishStoreRequestCommandData) -> CommandOutcome {
        let HandlePublishStoreRequestCommandData {
            operation_id,
            request,
            remote_peer_id,
            response: channel,
        } = data;
        let blockchain: BlockchainId = request.blockchain().clone();
        let dataset_root = request.dataset_root().to_string();
        let dataset_public = request.dataset().to_vec();
        let remote_peer_id = &remote_peer_id;
        tracing::Span::current().record("blockchain", tracing::field::display(&blockchain));
        tracing::Span::current().record("dataset_root", tracing::field::display(&dataset_root));

        // Validate that *this node* is in the shard for the given blockchain.
        // (The sender being in shard is not the relevant check here.)
        let local_peer_id = self.network_manager.peer_id();
        if !self
            .peer_registry
            .is_peer_in_shard(&blockchain, local_peer_id)
        {
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

        let computed_dataset_root = dkg_domain::calculate_merkle_root(&dataset_public);

        if dataset_root != computed_dataset_root {
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

        let identity_id = self.blockchain_manager.identity_id(&blockchain);

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

        let signature = match signature::sign_dataset_root_hex(
            self.blockchain_manager.as_ref(),
            &blockchain,
            dataset_root_hex,
        )
        .await
        {
            Ok(sig) => sig,
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

        let pending_dataset = Assertion {
            public: dataset_public,
            private: None,
        };
        let pending = PublishTmpDataset::new(pending_dataset, remote_peer_id.to_base58());
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

        tracing::info!(
            identity_id = %identity_id,
            "Store request validated; sending ACK response"
        );

        self.send_ack(channel, operation_id, identity_id, signature)
            .await;

        CommandOutcome::Completed
    }
}
