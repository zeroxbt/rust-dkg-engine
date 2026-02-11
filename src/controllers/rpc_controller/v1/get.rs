use std::sync::Arc;

use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::get::handle_get_request::HandleGetRequestCommandData, registry::Command,
        scheduler::CommandScheduler,
    },
    context::Context,
    managers::network::{
        PeerId,
        message::{RequestMessage, ResponseMessage},
        messages::{GetAck, GetRequestData},
        request_response,
    },
    state::ResponseChannels,
};

pub(crate) struct GetRpcController {
    response_channels: Arc<ResponseChannels<GetAck>>,
    command_scheduler: CommandScheduler,
}

impl GetRpcController {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            response_channels: Arc::clone(context.get_response_channels()),
            command_scheduler: context.command_scheduler().clone(),
        }
    }

    /// Handle an inbound get request.
    ///
    /// Returns `None` on success (channel stored for the command handler).
    /// Returns `Some(channel)` if the command queue is full — caller should
    /// send a Busy response.
    pub(crate) fn handle_request(
        &self,
        request: RequestMessage<GetRequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<GetAck>>,
        remote_peer_id: PeerId,
    ) -> Option<request_response::ResponseChannel<ResponseMessage<GetAck>>> {
        let RequestMessage { header, data } = request;

        let operation_id = header.operation_id();

        tracing::trace!(
            operation_id = %operation_id,
            ual = %data.ual(),
            peer = %remote_peer_id,
            "Get request received"
        );

        self.response_channels
            .store(&remote_peer_id, operation_id, channel);

        let command = Command::HandleGetRequest(HandleGetRequestCommandData::new(
            operation_id,
            data.ual().to_string(),
            data.token_ids().clone(),
            data.include_metadata(),
            data.paranet_ual().map(|s| s.to_string()),
            remote_peer_id,
        ));

        if !self
            .command_scheduler
            .try_schedule(CommandExecutionRequest::new(command))
        {
            return self.response_channels.retrieve(&remote_peer_id, operation_id);
        }

        None
    }
}
