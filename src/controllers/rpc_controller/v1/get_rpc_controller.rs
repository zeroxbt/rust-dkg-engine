use std::sync::Arc;

use crate::{
    commands::{
        command_executor::{CommandExecutionRequest, CommandScheduler},
        command_registry::Command,
        operations::get::protocols::get::handle_get_request_command::HandleGetRequestCommandData,
    },
    context::Context,
    controllers::rpc_controller::messages::{GetRequestData, GetResponseData},
    managers::network::{
        PeerId,
        message::{RequestMessage, ResponseMessage},
        request_response,
    },
    operations::GetOperation,
    services::{ResponseChannels, operation::OperationService as GenericOperationService},
};

pub(crate) struct GetRpcController {
    get_operation_service: Arc<GenericOperationService<GetOperation>>,
    response_channels: Arc<ResponseChannels<GetResponseData>>,
    command_scheduler: CommandScheduler,
}

impl GetRpcController {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            get_operation_service: Arc::clone(context.get_operation_service()),
            response_channels: Arc::clone(context.get_response_channels()),
            command_scheduler: context.command_scheduler().clone(),
        }
    }

    /// Returns a reference to the operation service for this controller.
    pub(crate) fn operation_service(&self) -> &Arc<GenericOperationService<GetOperation>> {
        &self.get_operation_service
    }

    pub(crate) async fn handle_request(
        &self,
        request: RequestMessage<GetRequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<GetResponseData>>,
        remote_peer_id: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let operation_id = header.operation_id();

        tracing::trace!(
            operation_id = %operation_id,
            ual = %data.ual(),
            peer = %remote_peer_id,
            "Get request received"
        );

        // Store channel for later retrieval by command handler
        self.response_channels
            .store(&remote_peer_id, operation_id, channel);

        // Schedule command to handle the get request
        let command = Command::HandleGetRequest(HandleGetRequestCommandData::new(
            operation_id,
            data.ual().to_string(),
            data.token_ids().clone(),
            data.include_metadata(),
            data.paranet_ual().map(|s| s.to_string()),
            data.content_type(),
            remote_peer_id,
        ));

        self.command_scheduler
            .schedule(CommandExecutionRequest::new(command))
            .await;
    }
}
