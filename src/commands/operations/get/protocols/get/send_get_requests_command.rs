use std::sync::Arc;

use libp2p::PeerId;
use network::{
    NetworkManager, ResponseMessage,
    message::{ResponseMessageHeader, ResponseMessageType},
    request_response::ResponseChannel,
};
use repository::RepositoryManager;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    controllers::rpc_controller::{messages::GetResponseData, NetworkProtocols, ProtocolResponse},
    services::ResponseChannels,
};

#[derive(Clone)]
pub struct SendGetRequestsCommandData {
    pub operation_id: Uuid,
    pub ual: String,
}

impl SendGetRequestsCommandData {
    pub fn new(
        operation_id: Uuid,
        ual: String,
        publish_operation_id: String,
        remote_peer_id: PeerId,
    ) -> Self {
        Self {
            operation_id,
            ual,
            publish_operation_id,
            remote_peer_id,
        }
    }
}

pub struct SendGetRequestsCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    response_channels: Arc<ResponseChannels<GetResponseData>>,
}

impl SendGetRequestsCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            response_channels: Arc::clone(context.get_response_channels()),
        }
    }
}

impl CommandHandler<SendGetRequestsCommandData> for SendGetRequestsCommandHandler {
    async fn execute(&self, data: &SendGetRequestsCommandData) -> CommandExecutionResult {
        CommandExecutionResult::Completed
    }
}
