use std::sync::Arc;

use dkg_domain::{Assertion, TokenIds};
use dkg_network::{GetAck, NetworkManager, PeerId, ResponseHandle};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    application::{HandleGetRequestInput, HandleGetRequestOutcome, HandleGetRequestWorkflow},
    commands::HandleGetRequestDeps,
    commands::{executor::CommandOutcome, registry::CommandHandler},
    node_state::ResponseChannels,
};

/// Command data for handling incoming get requests.
#[derive(Clone)]
pub(crate) struct HandleGetRequestCommandData {
    pub operation_id: Uuid,
    pub ual: String,
    pub token_ids: TokenIds,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub remote_peer_id: PeerId,
}

impl HandleGetRequestCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        ual: String,
        token_ids: TokenIds,
        include_metadata: bool,
        paranet_ual: Option<String>,
        remote_peer_id: PeerId,
    ) -> Self {
        Self {
            operation_id,
            ual,
            token_ids,
            include_metadata,
            paranet_ual,
            remote_peer_id,
        }
    }
}

pub(crate) struct HandleGetRequestCommandHandler {
    pub(super) network_manager: Arc<NetworkManager>,
    handle_get_request_workflow: Arc<HandleGetRequestWorkflow>,
    response_channels: Arc<ResponseChannels<GetAck>>,
}

impl HandleGetRequestCommandHandler {
    pub(crate) fn new(deps: HandleGetRequestDeps) -> Self {
        Self {
            network_manager: deps.network_manager,
            handle_get_request_workflow: deps.handle_get_request_workflow,
            response_channels: deps.get_response_channels,
        }
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseHandle<GetAck>,
        operation_id: Uuid,
        error_message: &str,
    ) {
        if let Err(e) = self
            .network_manager
            .send_get_nack(channel, operation_id, error_message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send get NACK response"
            );
        }
    }

    pub(crate) async fn send_ack(
        &self,
        channel: ResponseHandle<GetAck>,
        operation_id: Uuid,
        assertion: Assertion,
        metadata: Option<Vec<String>>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_get_ack(
                channel,
                operation_id,
                GetAck {
                    assertion,
                    metadata,
                },
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send get ACK response"
            );
        }
    }
}

impl CommandHandler<HandleGetRequestCommandData> for HandleGetRequestCommandHandler {
    #[instrument(
        name = "op.get.recv",
        skip(self, data),
        fields(
            operation_id = %data.operation_id,
            protocol = "get",
            direction = "recv",
            ual = %data.ual,
            remote_peer = %data.remote_peer_id,
            include_metadata = data.include_metadata,
            paranet = ?data.paranet_ual,
            effective_visibility = tracing::field::Empty,
        )
    )]
    async fn execute(&self, data: &HandleGetRequestCommandData) -> CommandOutcome {
        let operation_id = data.operation_id;
        let remote_peer_id = &data.remote_peer_id;

        // Retrieve the response channel
        let Some(channel) = self
            .response_channels
            .retrieve(remote_peer_id, operation_id)
        else {
            tracing::warn!(
                operation_id = %operation_id,
                peer = %remote_peer_id,
                "Response channel not found; request may have expired"
            );
            return CommandOutcome::Completed;
        };

        let input = HandleGetRequestInput {
            operation_id,
            ual: data.ual.clone(),
            token_ids: data.token_ids.clone(),
            include_metadata: data.include_metadata,
            paranet_ual: data.paranet_ual.clone(),
            remote_peer_id: *remote_peer_id,
            local_peer_id: *self.network_manager.peer_id(),
        };

        match self.handle_get_request_workflow.execute(&input).await {
            HandleGetRequestOutcome::Ack {
                assertion,
                metadata,
                effective_visibility,
            } => {
                tracing::Span::current().record(
                    "effective_visibility",
                    tracing::field::debug(&effective_visibility),
                );
                tracing::debug!(
                    operation_id = %operation_id,
                    "Found assertion data; sending ACK"
                );

                self.send_ack(channel, operation_id, assertion, metadata)
                    .await;
            }
            HandleGetRequestOutcome::Nack { error_message } => {
                self.send_nack(channel, operation_id, &error_message).await;
            }
        }

        CommandOutcome::Completed
    }
}
