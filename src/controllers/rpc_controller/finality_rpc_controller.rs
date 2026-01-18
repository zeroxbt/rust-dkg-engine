use std::sync::Arc;

use network::{
    PeerId,
    message::{RequestMessage, ResponseMessage},
    request_response,
};
use tokio::sync::mpsc::Sender;

use crate::{
    commands::{
        command_executor::CommandExecutionRequest, command_registry::Command,
        operations::publish::protocols::finality::handle_finality_request_command::HandleFinalityRequestCommandData,
    },
    context::Context,
    network::SessionManager,
    types::protocol::{FinalityRequestData, FinalityResponseData},
};

pub struct FinalityRpcController {
    session_manager: Arc<SessionManager<FinalityResponseData>>,
    schedule_command_tx: Sender<CommandExecutionRequest>,
}

impl FinalityRpcController {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            session_manager: Arc::clone(context.finality_session_manager()),
            schedule_command_tx: context.schedule_command_tx().clone(),
        }
    }

    pub async fn handle_request(
        &self,
        request: RequestMessage<FinalityRequestData>,
        channel: request_response::ResponseChannel<ResponseMessage<FinalityResponseData>>,
        remote_peer_id: PeerId,
    ) {
        let RequestMessage { header, data } = request;

        let operation_id = header.operation_id;

        tracing::trace!(
            operation_id = %operation_id,
            publish_operation_id = %data.publish_operation_id,
            ual = %data.ual,
            peer = %remote_peer_id,
            "Finality request received"
        );

        // Store channel in session manager for later retrieval by command handler
        self.session_manager
            .store_channel(&remote_peer_id, operation_id, channel);

        // Schedule the HandleFinalityRequest command
        let command_data = HandleFinalityRequestCommandData::new(
            operation_id,
            data.ual,
            data.publish_operation_id,
            remote_peer_id,
        );

        if let Err(e) = self
            .schedule_command_tx
            .send(CommandExecutionRequest::new(
                Command::HandleFinalityRequest(command_data),
            ))
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                error = %e,
                "Failed to schedule HandleFinalityRequest command"
            );
        }
    }

    pub async fn handle_response(
        &self,
        _response: ResponseMessage<FinalityResponseData>,
        _peer: PeerId,
    ) {
        // Do nothing on finality response
    }
}
