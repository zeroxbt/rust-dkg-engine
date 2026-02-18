use std::sync::Arc;

use dkg_blockchain::BlockchainId;
use dkg_domain::{Assertion, SignatureComponents};
use dkg_network::{NetworkManager, PeerId, ResponseHandle, StoreAck};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    application::{ServePublishStoreInput, ServePublishStoreOutcome, ServePublishStoreWorkflow},
    commands::HandlePublishStoreRequestDeps,
    commands::{executor::CommandOutcome, registry::CommandHandler},
    node_state::ResponseChannels,
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
    serve_publish_store_workflow: Arc<ServePublishStoreWorkflow>,
    response_channels: Arc<ResponseChannels<StoreAck>>,
}

impl HandlePublishStoreRequestCommandHandler {
    pub(crate) fn new(deps: HandlePublishStoreRequestDeps) -> Self {
        Self {
            network_manager: deps.network_manager,
            serve_publish_store_workflow: deps.serve_publish_store_workflow,
            response_channels: deps.store_response_channels,
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
        let remote_peer_id = &data.remote_peer_id;

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

        let input = ServePublishStoreInput {
            blockchain: data.blockchain.clone(),
            operation_id,
            dataset_root: data.dataset_root.clone(),
            remote_peer_id: *remote_peer_id,
            local_peer_id: *self.network_manager.peer_id(),
            dataset: data.dataset.clone(),
        };

        match self.serve_publish_store_workflow.execute(&input).await {
            ServePublishStoreOutcome::Ack {
                identity_id,
                signature,
            } => {
                tracing::debug!(
                    operation_id = %operation_id,
                    peer = %remote_peer_id,
                    identity_id = %identity_id,
                    "Sending ACK response with signature"
                );
                self.send_ack(channel, operation_id, identity_id, signature)
                    .await;
            }
            ServePublishStoreOutcome::Nack { error_message } => {
                self.send_nack(channel, operation_id, &error_message).await;
                return CommandOutcome::Completed;
            }
        }

        tracing::debug!(
            operation_id = %operation_id,
            peer = %remote_peer_id,
            "Store request validated; ACK sent"
        );

        CommandOutcome::Completed
    }
}
