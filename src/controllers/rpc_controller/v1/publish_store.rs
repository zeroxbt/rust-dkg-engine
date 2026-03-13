use dkg_network::{InboundRequest, ResponseHandle, StoreAck, StoreRequestData};

use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::publish::store::handle_publish_store_request::HandlePublishStoreRequestCommandData,
        registry::Command, scheduler::CommandScheduler,
    },
    controllers::rpc_controller::PublishStoreRpcControllerDeps,
};

pub(crate) struct PublishStoreRpcController {
    command_scheduler: CommandScheduler,
}

impl PublishStoreRpcController {
    pub(crate) fn new(deps: PublishStoreRpcControllerDeps) -> Self {
        Self {
            command_scheduler: deps.command_scheduler,
        }
    }

    /// Handle an inbound publish store request.
    ///
    /// Returns `None` on success (response handle stored for the command handler).
    /// Returns `Some(response_handle)` if the command queue is full — caller should
    /// send a Busy response.
    pub(crate) fn handle_request(
        &self,
        request: InboundRequest<StoreRequestData>,
        response_handle: ResponseHandle<StoreAck>,
    ) -> Option<ResponseHandle<StoreAck>> {
        tracing::trace!(
            operation_id = %request.operation_id(),
            dataset_root = %request.data().dataset_root(),
            peer = %request.peer_id(),
            "Store request received"
        );

        let command = Command::HandlePublishStoreRequest(
            HandlePublishStoreRequestCommandData::new(request, response_handle),
        );
        match self
            .command_scheduler
            .try_schedule(CommandExecutionRequest::new(command))
        {
            Ok(()) => None,
            Err(request) => match request.into_command() {
                Command::HandlePublishStoreRequest(command) => Some(command.response_handle),
                _ => unreachable!("unexpected rejected command type"),
            },
        }
    }
}
