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
    /// Returns `None` on success (channel stored for the command handler).
    /// Returns `Some(channel)` if the command queue is full — caller should
    /// send a Busy response.
    pub(crate) fn handle_request(
        &self,
        request: InboundRequest<BatchGetRequestData>,
        channel: ResponseHandle<BatchGetAck>,
    ) -> Option<ResponseHandle<BatchGetAck>> {
        let operation_id = request.operation_id();
        let remote_peer_id = *request.peer_id();
        let data = request.into_data();

        tracing::trace!(
            operation_id = %operation_id,
            ual_count = data.uals().len(),
            peer = %remote_peer_id,
            "Batch get request received"
        );

        let command = Command::HandleBatchGetRequest(HandleBatchGetRequestCommandData::new(
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
                Command::HandleBatchGetRequest(command) => Some(command.response),
                _ => unreachable!("unexpected rejected command type"),
            },
        }
    }
}
