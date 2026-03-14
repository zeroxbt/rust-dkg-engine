use std::sync::Arc;

use dkg_network::{
    FinalityAck, FinalityRequestData, InboundRequest, NetworkManager, ResponseHandle,
};
use dkg_repository::FinalityStatusRepository;
use tracing::instrument;
use uuid::Uuid;

use crate::commands::{
    HandlePublishFinalityRequestDeps,
    executor::CommandOutcome,
    registry::{CommandHandler, InboundCommandData},
};

/// Command data for handling incoming publish finality requests from storage nodes.
/// This runs on the publisher node when a storage node confirms it has stored the data.
pub(crate) struct HandlePublishFinalityRequestCommandData {
    /// Inbound finality request payload.
    pub request: InboundRequest<FinalityRequestData>,
    pub response_handle: ResponseHandle<FinalityAck>,
}

impl HandlePublishFinalityRequestCommandData {
    pub(crate) fn new(
        request: InboundRequest<FinalityRequestData>,
        response_handle: ResponseHandle<FinalityAck>,
    ) -> Self {
        Self {
            request,
            response_handle,
        }
    }
}

impl InboundCommandData<FinalityAck> for HandlePublishFinalityRequestCommandData {
    fn into_response_handle(self) -> ResponseHandle<FinalityAck> {
        self.response_handle
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
        response_handle: ResponseHandle<FinalityAck>,
        operation_id: Uuid,
        ual: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_finality_ack(
                response_handle,
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
        response_handle: ResponseHandle<FinalityAck>,
        operation_id: Uuid,
        ual: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_finality_nack(
                response_handle,
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

impl CommandHandler for HandlePublishFinalityRequestCommandHandler {
    type Data = HandlePublishFinalityRequestCommandData;

    #[instrument(
        name = "op.publish_finality.recv",
        skip(self, data),
        fields(
            operation_id = %data.request.operation_id(),
            protocol = "publish_finality",
            direction = "recv",
            publish_operation_id = tracing::field::Empty,
            ual = tracing::field::Empty,
            remote_peer = %data.request.peer_id(),
        )
    )]
    async fn execute(&self, data: Self::Data) -> CommandOutcome {
        let HandlePublishFinalityRequestCommandData {
            request,
            response_handle,
        } = data;
        let (publish_finality_operation_id, remote_peer_id, request) = request.into_parts();
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
                self.send_nack(response_handle, publish_finality_operation_id, &ual)
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
            self.send_nack(response_handle, publish_finality_operation_id, &ual)
                .await;
            return CommandOutcome::Completed;
        }

        self.send_ack(response_handle, publish_finality_operation_id, &ual)
            .await;

        tracing::info!("Finality request handled");

        CommandOutcome::Completed
    }
}
