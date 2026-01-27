use std::sync::Arc;

use crate::{
    commands::{
        command_executor::{CommandExecutionRequest, CommandScheduler},
        command_registry::Command,
        operations::publish::protocols::store::handle_store_request_command::HandleStoreRequestCommandData,
    },
    context::Context,
    controllers::rpc_controller::messages::{StoreRequestData, StoreResponseData},
    managers::{
        network::{
            PeerId,
            message::{RequestMessage, ResponseMessage},
            request_response,
        },
        triple_store::Assertion,
    },
    services::ResponseChannels,
};

pub(crate) struct StoreRpcController {
    response_channels: Arc<ResponseChannels<StoreResponseData>>,
    command_scheduler: CommandScheduler,
}

impl StoreRpcController {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            response_channels: Arc::clone(context.store_response_channels()),
            command_scheduler: context.command_scheduler().clone(),
        }
    }

    pub(crate) async fn handle_request(
        &self,
        request: RequestMessage<StoreRequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<StoreResponseData>>,
        remote_peer_id: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let operation_id = header.operation_id();

        tracing::trace!(
            operation_id = %operation_id,
            dataset_root = %data.dataset_root(),
            peer = %remote_peer_id,
            "Store request received"
        );

        // Store channel for later retrieval by command handler
        self.response_channels
            .store(&remote_peer_id, operation_id, channel);

        // Create dataset from request data
        let dataset = Assertion {
            public: data.dataset().to_owned(),
            private: None,
        };

        // Schedule command with dataset passed inline
        let command = Command::HandleStoreRequest(HandleStoreRequestCommandData::new(
            data.blockchain().clone(),
            operation_id,
            data.dataset_root().to_owned(),
            remote_peer_id,
            dataset,
        ));

        self.command_scheduler
            .schedule(CommandExecutionRequest::new(command))
            .await;
    }
}
