use std::{collections::HashMap, sync::Arc};

use dkg_domain::{Assertion, TokenIds};
use dkg_network::{BatchGetAck, NetworkManager, PeerId, ResponseHandle};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    application::{ServeBatchGetInput, ServeBatchGetOutcome, ServeBatchGetWorkflow},
    commands::HandleBatchGetRequestDeps,
    commands::{executor::CommandOutcome, registry::CommandHandler},
    node_state::ResponseChannels,
};

/// Command data for handling incoming batch get requests.
#[derive(Clone)]
pub(crate) struct HandleBatchGetRequestCommandData {
    pub operation_id: Uuid,
    pub uals: Vec<String>,
    pub token_ids: HashMap<String, TokenIds>,
    pub include_metadata: bool,
    pub remote_peer_id: PeerId,
}

impl HandleBatchGetRequestCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        uals: Vec<String>,
        token_ids: HashMap<String, TokenIds>,
        include_metadata: bool,
        remote_peer_id: PeerId,
    ) -> Self {
        Self {
            operation_id,
            uals,
            token_ids,
            include_metadata,
            remote_peer_id,
        }
    }
}

pub(crate) struct HandleBatchGetRequestCommandHandler {
    pub(super) network_manager: Arc<NetworkManager>,
    serve_batch_get_workflow: Arc<ServeBatchGetWorkflow>,
    response_channels: Arc<ResponseChannels<BatchGetAck>>,
}

impl HandleBatchGetRequestCommandHandler {
    pub(crate) fn new(deps: HandleBatchGetRequestDeps) -> Self {
        Self {
            network_manager: deps.network_manager,
            serve_batch_get_workflow: deps.serve_batch_get_workflow,
            response_channels: deps.batch_get_response_channels,
        }
    }

    pub(crate) async fn send_ack(
        &self,
        channel: ResponseHandle<BatchGetAck>,
        operation_id: Uuid,
        assertions: HashMap<String, Assertion>,
        metadata: HashMap<String, Vec<String>>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_batch_get_ack(
                channel,
                operation_id,
                BatchGetAck {
                    assertions,
                    metadata,
                },
            )
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send batch-get ACK response"
            );
        }
    }

    pub(crate) async fn send_nack(
        &self,
        channel: ResponseHandle<BatchGetAck>,
        operation_id: Uuid,
        message: impl Into<String>,
    ) {
        if let Err(e) = self
            .network_manager
            .send_batch_get_nack(channel, operation_id, message)
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to send batch-get NACK response"
            );
        }
    }
}

impl CommandHandler<HandleBatchGetRequestCommandData> for HandleBatchGetRequestCommandHandler {
    #[instrument(
        name = "op.batch_get.recv",
        skip(self, data),
        fields(
            operation_id = %data.operation_id,
            protocol = "batch_get",
            direction = "recv",
            remote_peer = %data.remote_peer_id,
            include_metadata = data.include_metadata,
            ual_count = tracing::field::Empty,
        )
    )]
    async fn execute(&self, data: &HandleBatchGetRequestCommandData) -> CommandOutcome {
        let operation_id = data.operation_id;
        let remote_peer_id = &data.remote_peer_id;
        let bounded_ual_count = data.uals.len().min(crate::application::UAL_MAX_LIMIT);

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

        let input = ServeBatchGetInput {
            operation_id,
            uals: data.uals.clone(),
            token_ids: data.token_ids.clone(),
            include_metadata: data.include_metadata,
            local_peer_id: *self.network_manager.peer_id(),
        };

        tracing::Span::current().record("ual_count", tracing::field::display(bounded_ual_count));

        match self.serve_batch_get_workflow.execute(&input).await {
            ServeBatchGetOutcome::Ack {
                assertions,
                metadata,
            } => {
                tracing::debug!(
                    operation_id = %operation_id,
                    assertions_count = assertions.len(),
                    metadata_count = metadata.len(),
                    "Sending batch get response"
                );
                self.send_ack(channel, operation_id, assertions, metadata)
                    .await;
            }
            ServeBatchGetOutcome::Nack { error_message } => {
                self.send_nack(channel, operation_id, error_message).await;
            }
        }

        CommandOutcome::Completed
    }
}
