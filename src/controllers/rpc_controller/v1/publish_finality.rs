use std::sync::Arc;

use dkg_network::{
    PeerId,
    message::{RequestMessage, ResponseMessage},
    messages::{FinalityAck, FinalityRequestData},
    request_response,
};

use super::inbound_request::store_channel_and_try_schedule;
use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::publish::finality::handle_publish_finality_request::HandlePublishFinalityRequestCommandData,
        registry::Command, scheduler::CommandScheduler,
    },
    context::Context,
    state::ResponseChannels,
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

    /// Handle an inbound publish finality request.
    ///
    /// Returns `None` on success (channel stored for the command handler).
    /// Returns `Some(channel)` if the command queue is full — caller should
    /// send a Busy response.
    pub(crate) fn handle_request(
        &self,
        request: RequestMessage<FinalityRequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<FinalityAck>>,
        remote_peer_id: PeerId,
    ) -> Option<request_response::ResponseChannel<ResponseMessage<FinalityAck>>> {
        let RequestMessage { header, data } = request;

        let operation_id = header.operation_id();

        tracing::trace!(
            operation_id = %operation_id,
            publish_operation_id = %data.publish_operation_id(),
            ual = %data.ual(),
            peer = %remote_peer_id,
            "Finality request received"
        );

        let command_data = HandlePublishFinalityRequestCommandData::new(
            operation_id,
            data.ual().to_string(),
            data.publish_operation_id().to_string(),
            remote_peer_id,
        );
        store_channel_and_try_schedule(
            &self.response_channels,
            &self.command_scheduler,
            &remote_peer_id,
            operation_id,
            channel,
            CommandExecutionRequest::new(Command::HandlePublishFinalityRequest(command_data)),
        )
    }
}
