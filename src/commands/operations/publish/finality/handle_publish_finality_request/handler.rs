use std::sync::Arc;

use dkg_network::{FinalityAck, FinalityRequestData, NetworkManager, PeerId, ResponseHandle};
use dkg_repository::FinalityStatusRepository;
use tracing::instrument;
use uuid::Uuid;

use crate::commands::{
    HandlePublishFinalityRequestDeps, executor::CommandOutcome, registry::CommandHandler,
};

/// Command data for handling incoming publish finality requests from storage nodes.
/// This runs on the publisher node when a storage node confirms it has stored the data.
pub(crate) struct HandlePublishFinalityRequestCommandData {
    /// The operation ID for this finality request
    pub operation_id: Uuid,
    /// Inbound finality request payload.
    pub request: FinalityRequestData,
    /// The peer ID of the storage node sending the finality request
    pub remote_peer_id: PeerId,
    pub response: ResponseHandle<FinalityAck>,
}

impl HandlePublishFinalityRequestCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        request: FinalityRequestData,
        remote_peer_id: PeerId,
        response: ResponseHandle<FinalityAck>,
    ) -> Self {
        Self {
            operation_id,
            request,
            remote_peer_id,
            response,
        }
    }
}

pub(crate) struct HandlePublishFinalityRequestCommandHandler {
    finality_status_repository: FinalityStatusRepository,
    pub(super) network_manager: Arc<NetworkManager>,
}

impl HandlePublishFinalityRequestCommandHandler {
    pub(crate) fn new(deps: HandlePublishFinalityRequestDeps) -> Self {
        Self {
            finality_status_repository: deps.finality_status_repository,
            network_manager: deps.network_manager,
        }
    }

    pub(crate) async fn send_ack(
        &self,
        channel: ResponseHandle<FinalityAck>,
        operation_id: Uuid,
        ual: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_finality_ack(
                channel,
                operation_id,
                FinalityAck {
                    message: format!("Acknowledged storing of {}", ual),
                },
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send finality ACK response"
            );
        }
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseHandle<FinalityAck>,
        operation_id: Uuid,
        ual: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_finality_nack(
                channel,
                operation_id,
                format!("Failed to acknowledge storing of {}", ual),
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send finality NACK response"
            );
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
            publish_operation_id = tracing::field::Empty,
            ual = tracing::field::Empty,
            remote_peer = %data.remote_peer_id,
        )
    )]
    async fn execute(&self, data: HandlePublishFinalityRequestCommandData) -> CommandOutcome {
        let HandlePublishFinalityRequestCommandData {
            operation_id: publish_finality_operation_id,
            request,
            remote_peer_id,
            response: channel,
        } = data;
        let ual = request.ual().to_string();
        let publish_store_operation_id = request.publish_operation_id().to_string();
        let remote_peer_id = &remote_peer_id;
        tracing::Span::current().record(
            "publish_operation_id",
            tracing::field::display(&publish_store_operation_id),
        );
        tracing::Span::current().record("ual", tracing::field::display(&ual));

        // Parse the publish operation ID
        let publish_store_operation_id = match Uuid::parse_str(&publish_store_operation_id) {
            Ok(uuid) => uuid,
            Err(e) => {
                tracing::error!(
                    operation_id = %publish_finality_operation_id,
                    publish_store_operation_id = %publish_store_operation_id,
                    error = %e,
                    "Failed to parse publish_operation_id as UUID"
                );
                self.send_nack(channel, publish_finality_operation_id, &ual)
                    .await;
                return CommandOutcome::Completed;
            }
        };

        // Save the finality ack to the database
        if let Err(e) = self
            .finality_status_repository
            .save_finality_ack(
                publish_store_operation_id,
                &ual,
                &remote_peer_id.to_base58(),
            )
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
            self.send_nack(channel, publish_finality_operation_id, &ual)
                .await;
            return CommandOutcome::Completed;
        }

        self.send_ack(channel, publish_finality_operation_id, &ual)
            .await;

        tracing::info!("Finality request handled");

        CommandOutcome::Completed
    }
}
