use std::sync::Arc;

use crate::{
    commands::{
        command_executor::{CommandExecutionRequest, CommandScheduler},
        command_registry::Command,
        operations::publish::protocols::finality::handle_finality_request_command::HandleFinalityRequestCommandData,
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

pub(crate) struct FinalityRpcController {
    response_channels: Arc<ResponseChannels<FinalityAck>>,
    command_scheduler: CommandScheduler,
}

impl FinalityRpcController {
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

        // Schedule the HandleFinalityRequest command
        let command_data = HandleFinalityRequestCommandData::new(
            operation_id,
            data.ual().to_string(),
            data.publish_operation_id().to_string(),
            remote_peer_id,
        );

        self.command_scheduler
            .schedule(CommandExecutionRequest::new(
                Command::HandleFinalityRequest(command_data),
            ))
            .await;
    }
}
