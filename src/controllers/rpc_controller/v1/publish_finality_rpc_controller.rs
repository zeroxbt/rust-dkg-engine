use std::sync::Arc;

use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::publish::finality::handle_publish_finality_request::HandlePublishFinalityRequestCommandData,
        registry::Command, scheduler::CommandScheduler,
    },
    context::Context,
    controllers::rpc_controller::messages::{FinalityAck, FinalityRequestData},
    managers::network::{
        PeerId,
        message::{RequestMessage, ResponseMessage},
        request_response,
    },
    services::ResponseChannels,
};

pub(crate) struct PublishFinalityRpcController {
    response_channels: Arc<ResponseChannels<FinalityAck>>,
    command_scheduler: CommandScheduler,
}

impl PublishFinalityRpcController {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            response_channels: Arc::clone(context.finality_response_channels()),
            command_scheduler: context.command_scheduler().clone(),
        }
    }

    pub(crate) async fn handle_request(
        &self,
        request: RequestMessage<FinalityRequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<FinalityAck>>,
        remote_peer_id: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let operation_id = header.operation_id();

        tracing::trace!(
            operation_id = %operation_id,
            publish_operation_id = %data.publish_operation_id(),
            ual = %data.ual(),
            peer = %remote_peer_id,
            "Finality request received"
        );

        // Store channel for later retrieval by command handler
        self.response_channels
            .store(&remote_peer_id, operation_id, channel);

        // Schedule the HandlePublishFinalityRequest command
        let command_data = HandlePublishFinalityRequestCommandData::new(
            operation_id,
            data.ual().to_string(),
            data.publish_operation_id().to_string(),
            remote_peer_id,
        );

        self.command_scheduler
            .schedule(CommandExecutionRequest::new(
                Command::HandlePublishFinalityRequest(command_data),
            ))
            .await;
    }
}
