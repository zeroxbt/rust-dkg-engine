use dkg_network::{FinalityAck, FinalityRequestData, InboundRequest, ResponseHandle};

use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::publish::finality::handle_publish_finality_request::HandlePublishFinalityRequestCommandData,
        registry::Command, scheduler::CommandScheduler,
    },
    controllers::rpc_controller::PublishFinalityRpcControllerDeps,
};

pub(crate) struct PublishFinalityRpcController {
    command_scheduler: CommandScheduler,
}

impl PublishFinalityRpcController {
    pub(crate) fn new(deps: PublishFinalityRpcControllerDeps) -> Self {
        Self {
            command_scheduler: deps.command_scheduler,
        }
    }

    /// Handle an inbound publish finality request.
    ///
    /// Returns `None` on success (response handle stored for the command handler).
    /// Returns `Some(response_handle)` if the command queue is full — caller should
    /// send a Busy response.
    pub(crate) fn handle_request(
        &self,
        request: InboundRequest<FinalityRequestData>,
        response_handle: ResponseHandle<FinalityAck>,
    ) -> Option<ResponseHandle<FinalityAck>> {
        tracing::trace!(
            operation_id = %request.operation_id(),
            publish_operation_id = %request.data().publish_operation_id(),
            ual = %request.data().ual(),
            peer = %request.peer_id(),
            "Finality request received"
        );

        let command_data = HandlePublishFinalityRequestCommandData::new(request, response_handle);
        match self
            .command_scheduler
            .try_schedule(CommandExecutionRequest::new(
                Command::HandlePublishFinalityRequest(command_data),
            )) {
            Ok(()) => None,
            Err(request) => match request.into_command() {
                Command::HandlePublishFinalityRequest(command) => Some(command.response_handle),
                _ => unreachable!("unexpected rejected command type"),
            },
        }
    }
}
