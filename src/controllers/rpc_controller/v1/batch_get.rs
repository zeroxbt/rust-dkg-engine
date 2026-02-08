use std::sync::Arc;

use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::batch_get::handle_batch_get_request::HandleBatchGetRequestCommandData,
        registry::Command, scheduler::CommandScheduler,
    },
    context::Context,
    managers::network::{
        PeerId,
        message::{RequestMessage, ResponseMessage},
        messages::{BatchGetAck, BatchGetRequestData},
        request_response,
    },
    state::ResponseChannels,
};

pub(crate) struct BatchGetRpcController {
    response_channels: Arc<ResponseChannels<BatchGetAck>>,
    command_scheduler: CommandScheduler,
}

impl BatchGetRpcController {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            response_channels: Arc::clone(context.batch_get_response_channels()),
            command_scheduler: context.command_scheduler().clone(),
        }
    }

    pub(crate) async fn handle_request(
        &self,
        request: RequestMessage<BatchGetRequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<BatchGetAck>>,
        remote_peer_id: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let operation_id = header.operation_id();

        tracing::trace!(
            operation_id = %operation_id,
            ual_count = data.uals().len(),
            peer = %remote_peer_id,
            "Batch get request received"
        );

        // Store channel for later retrieval by command handler
        self.response_channels
            .store(&remote_peer_id, operation_id, channel);

        // Schedule command to handle the batch get request
        let command = Command::HandleBatchGetRequest(HandleBatchGetRequestCommandData::new(
            operation_id,
            data.uals().to_vec(),
            data.token_ids().clone(),
            data.include_metadata(),
            remote_peer_id,
        ));

        self.command_scheduler
            .schedule(CommandExecutionRequest::new(command))
            .await;
    }
}
