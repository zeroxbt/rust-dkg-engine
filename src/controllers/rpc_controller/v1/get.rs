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
    /// Returns `None` on success (response handle stored for the command handler).
    /// Returns `Some(response_handle)` if the command queue is full — caller should
    /// send a Busy response.
    pub(crate) fn handle_request(
        &self,
        request: InboundRequest<GetRequestData>,
        response_handle: ResponseHandle<GetAck>,
    ) -> Option<ResponseHandle<GetAck>> {
        tracing::trace!(
            operation_id = %request.operation_id(),
            ual = %request.data().ual(),
            peer = %request.peer_id(),
            "Get request received"
        );

        let command =
            Command::HandleGetRequest(HandleGetRequestCommandData::new(request, response_handle));
        match self
            .command_scheduler
            .try_schedule(CommandExecutionRequest::new(command))
        {
            Ok(()) => None,
            Err(request) => match request.into_command() {
                Command::HandleGetRequest(command) => Some(command.response_handle),
                _ => unreachable!("unexpected rejected command type"),
            },
        }
    }
}
