use std::sync::Arc;

use dkg_domain::Assertion;
use dkg_network::{InboundRequest, ResponseHandle, StoreAck, StoreRequestData};

use super::inbound_request::store_channel_and_try_schedule;
use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::publish::store::handle_publish_store_request::HandlePublishStoreRequestCommandData,
        registry::Command, scheduler::CommandScheduler,
    },
    context::Context,
    state::ResponseChannels,
};

pub(crate) struct PublishStoreRpcController {
    response_channels: Arc<ResponseChannels<StoreAck>>,
    command_scheduler: CommandScheduler,
}

impl PublishStoreRpcController {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            response_channels: Arc::clone(context.store_response_channels()),
            command_scheduler: context.command_scheduler().clone(),
        }
    }

    /// Handle an inbound publish store request.
    ///
    /// Returns `None` on success (channel stored for the command handler).
    /// Returns `Some(channel)` if the command queue is full — caller should
    /// send a Busy response.
    pub(crate) fn handle_request(
        &self,
        request: InboundRequest<StoreRequestData>,
        channel: ResponseHandle<StoreAck>,
    ) -> Option<ResponseHandle<StoreAck>> {
        let operation_id = request.operation_id();
        let remote_peer_id = request.peer_id().clone();
        let data = request.into_data();

        tracing::trace!(
            operation_id = %operation_id,
            dataset_root = %data.dataset_root(),
            peer = %remote_peer_id,
            "Store request received"
        );

        let dataset = Assertion {
            public: data.dataset().to_owned(),
            private: None,
        };

        let command =
            Command::HandlePublishStoreRequest(HandlePublishStoreRequestCommandData::new(
                data.blockchain().clone(),
                operation_id,
                data.dataset_root().to_owned(),
                remote_peer_id,
                dataset,
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
