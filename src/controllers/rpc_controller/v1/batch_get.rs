use dkg_network::{BatchGetAck, BatchGetRequestData, InboundRequest, ResponseHandle};

use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::batch_get::handle_batch_get_request::HandleBatchGetRequestCommandData,
        registry::Command, scheduler::CommandScheduler,
    },
    controllers::rpc_controller::BatchGetRpcControllerDeps,
};

pub(crate) struct BatchGetRpcController {
    command_scheduler: CommandScheduler,
}

impl BatchGetRpcController {
    pub(crate) fn new(deps: BatchGetRpcControllerDeps) -> Self {
        Self {
            command_scheduler: deps.command_scheduler,
        }
    }

    /// Handle an inbound batch get request.
    ///
    /// Returns `None` on success (response handle stored for the command handler).
    /// Returns `Some(response_handle)` if the command queue is full — caller should
    /// send a Busy response.
    pub(crate) fn handle_request(
        &self,
        request: InboundRequest<BatchGetRequestData>,
        response_handle: ResponseHandle<BatchGetAck>,
    ) -> Option<ResponseHandle<BatchGetAck>> {
        tracing::trace!(
            operation_id = %request.operation_id(),
            ual_count = request.data().uals().len(),
            peer = %request.peer_id(),
            "Batch get request received"
        );

        let command = Command::HandleBatchGetRequest(HandleBatchGetRequestCommandData::new(
            request,
            response_handle,
        ));
        match self
            .command_scheduler
            .try_schedule(CommandExecutionRequest::new(command))
        {
            Ok(()) => None,
            Err(request) => match request.into_command() {
                Command::HandleBatchGetRequest(command) => Some(command.response_handle),
                _ => unreachable!("unexpected rejected command type"),
            },
        }
    }
}
