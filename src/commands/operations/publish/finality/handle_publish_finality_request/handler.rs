use std::sync::Arc;

use dkg_network::{FinalityAck, NetworkManager, PeerId};
use dkg_repository::RepositoryManager;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    commands::{executor::CommandOutcome, registry::CommandHandler},
    context::Context,
    state::ResponseChannels,
};

/// Command data for handling incoming publish finality requests from storage nodes.
/// This runs on the publisher node when a storage node confirms it has stored the data.
#[derive(Clone)]
pub(crate) struct HandlePublishFinalityRequestCommandData {
    /// The operation ID for this finality request
    pub operation_id: Uuid,
    /// The UAL (Universal Asset Locator) of the knowledge collection
    pub ual: String,
    /// The original publish operation ID
    pub publish_store_operation_id: String,
    /// The peer ID of the storage node sending the finality request
    pub remote_peer_id: PeerId,
}

impl HandlePublishFinalityRequestCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        ual: String,
        publish_store_operation_id: String,
        remote_peer_id: PeerId,
    ) -> Self {
        Self {
            operation_id,
            ual,
            publish_store_operation_id,
            remote_peer_id,
        }
    }
}

pub(crate) struct HandlePublishFinalityRequestCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    pub(super) network_manager: Arc<NetworkManager>,
    response_channels: Arc<ResponseChannels<FinalityAck>>,
}

impl HandlePublishFinalityRequestCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            response_channels: Arc::clone(context.finality_response_channels()),
        }
    }
}

impl CommandHandler<HandlePublishFinalityRequestCommandData>
    for HandlePublishFinalityRequestCommandHandler
{
    #[instrument(
        name = "op.publish_finality.recv",
        skip(self, data),
        fields(
            operation_id = %data.operation_id,
            protocol = "publish_finality",
            direction = "recv",
            publish_operation_id = %data.publish_store_operation_id,
            ual = %data.ual,
            remote_peer = %data.remote_peer_id,
        )
    )]
    async fn execute(&self, data: &HandlePublishFinalityRequestCommandData) -> CommandOutcome {
        let publish_finality_operation_id = data.operation_id;
        let ual = &data.ual;
        let publish_store_operation_id = &data.publish_store_operation_id;
        let remote_peer_id = &data.remote_peer_id;

        // Retrieve the response channel
        let Some(channel) = self
            .response_channels
            .retrieve(remote_peer_id, publish_finality_operation_id)
        else {
            tracing::warn!(
                operation_id = %publish_finality_operation_id,
                peer = %remote_peer_id,
                "Response channel not found; finality request may have expired"
            );
            return CommandOutcome::Completed;
        };

        // Parse the publish operation ID
        let publish_store_operation_id = match Uuid::parse_str(publish_store_operation_id) {
            Ok(uuid) => uuid,
            Err(e) => {
                tracing::error!(
                    operation_id = %publish_finality_operation_id,
                    publish_store_operation_id = %publish_store_operation_id,
                    error = %e,
                    "Failed to parse publish_operation_id as UUID"
                );
                self.send_nack(channel, publish_finality_operation_id, ual)
                    .await;
                return CommandOutcome::Completed;
            }
        };

        // Save the finality ack to the database
        if let Err(e) = self
            .repository_manager
            .finality_status_repository()
            .save_finality_ack(publish_store_operation_id, ual, &remote_peer_id.to_base58())
            .await
        {
            tracing::error!(
                operation_id = %publish_finality_operation_id,
                publish_operation_id = %publish_store_operation_id,
                ual = %ual,
                peer = %remote_peer_id,
                error = %e,
                "Failed to save finality ack"
            );
            self.send_nack(channel, publish_finality_operation_id, ual)
                .await;
            return CommandOutcome::Completed;
        }

        tracing::debug!(
            operation_id = %publish_finality_operation_id,
            publish_operation_id = %publish_store_operation_id,
            ual = %ual,
            peer = %remote_peer_id,
            "Finality ack saved successfully"
        );

        // Send ACK response back to the storage node
        tracing::debug!(
            operation_id = %publish_finality_operation_id,
            peer = %remote_peer_id,
            "Sending finality ACK response"
        );

        self.send_ack(channel, publish_finality_operation_id, ual)
            .await;

        tracing::debug!(
            operation_id = %publish_finality_operation_id,
            peer = %remote_peer_id,
            ual = %ual,
            "Finality request handled"
        );

        CommandOutcome::Completed
    }
}
