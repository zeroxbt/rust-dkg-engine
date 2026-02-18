use std::sync::Arc;

use dkg_network::{BatchGetAck, BatchGetRequestData, InboundRequest, ResponseHandle};

use super::inbound_request::store_channel_and_try_schedule;
use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::batch_get::handle_batch_get_request::HandleBatchGetRequestCommandData,
        registry::Command, scheduler::CommandScheduler,
    },
    controllers::rpc_controller::BatchGetRpcControllerDeps,
    node_state::ResponseChannels,
};

pub(crate) struct BatchGetRpcController {
    response_channels: Arc<ResponseChannels<BatchGetAck>>,
    command_scheduler: CommandScheduler,
}

impl BatchGetRpcController {
    pub(crate) fn new(deps: BatchGetRpcControllerDeps) -> Self {
        Self {
            response_channels: deps.batch_get_response_channels,
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
            data.uals().to_vec(),
            data.token_ids().clone(),
            data.include_metadata(),
            remote_peer_id,
        ));
        store_channel_and_try_schedule(
            &self.response_channels,
            &self.command_scheduler,
            &remote_peer_id,
            operation_id,
            channel,
            CommandExecutionRequest::new(command),
        )
    }
}
