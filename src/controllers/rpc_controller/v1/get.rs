use dkg_network::{GetAck, GetRequestData, InboundRequest, ResponseHandle};

use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::get::handle_get_request::HandleGetRequestCommandData, registry::Command,
        scheduler::CommandScheduler,
    },
    controllers::rpc_controller::GetRpcControllerDeps,
};

pub(crate) struct GetRpcController {
    command_scheduler: CommandScheduler,
}

impl GetRpcController {
    pub(crate) fn new(deps: GetRpcControllerDeps) -> Self {
        Self {
            command_scheduler: deps.command_scheduler,
        }
    }

    /// Handle an inbound get request.
    ///
    /// Returns `None` on success (channel stored for the command handler).
    /// Returns `Some(channel)` if the command queue is full — caller should
    /// send a Busy response.
    pub(crate) fn handle_request(
        &self,
        request: InboundRequest<GetRequestData>,
        channel: ResponseHandle<GetAck>,
    ) -> Option<ResponseHandle<GetAck>> {
        let operation_id = request.operation_id();
        let remote_peer_id = *request.peer_id();
        let data = request.into_data();

        tracing::trace!(
            operation_id = %operation_id,
            ual = %data.ual(),
            peer = %remote_peer_id,
            "Get request received"
        );

        let command = Command::HandleGetRequest(HandleGetRequestCommandData::new(
            operation_id,
            data,
            remote_peer_id,
            channel,
        ));
        match self
            .command_scheduler
            .try_schedule(CommandExecutionRequest::new(command))
        {
            Ok(()) => None,
            Err(request) => match request.into_command() {
                Command::HandleGetRequest(command) => Some(command.response),
                _ => unreachable!("unexpected rejected command type"),
            },
        }
    }
}
