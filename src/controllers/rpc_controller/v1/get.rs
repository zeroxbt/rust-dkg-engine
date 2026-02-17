use std::sync::Arc;

use dkg_network::{
    InboundRequest, ResponseHandle,
    messages::{GetAck, GetRequestData},
};

use super::inbound_request::store_channel_and_try_schedule;
use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::get::handle_get_request::HandleGetRequestCommandData, registry::Command,
        scheduler::CommandScheduler,
    },
    context::Context,
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
        request: InboundRequest<GetRequestData>,
        channel: ResponseHandle<GetAck>,
    ) -> Option<ResponseHandle<GetAck>> {
        let operation_id = request.operation_id();
        let remote_peer_id = request.peer_id().clone();
        let data = request.into_data();

        tracing::trace!(
            operation_id = %operation_id,
            ual = %data.ual(),
            peer = %remote_peer_id,
            "Get request received"
        );

        let command = Command::HandleGetRequest(HandleGetRequestCommandData::new(
            operation_id,
            data.ual().to_string(),
            data.token_ids().clone(),
            data.include_metadata(),
            data.paranet_ual().map(|s| s.to_string()),
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
