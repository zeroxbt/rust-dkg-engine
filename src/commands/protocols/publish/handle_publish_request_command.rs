use std::sync::Arc;

use async_trait::async_trait;
use blockchain::{BlockchainName, utils::SignatureComponents};
use network::{
    NetworkManager, ResponseMessage,
    message::{ResponseMessageHeader, ResponseMessageType},
};
use repository::RepositoryManager;
use serde::{Deserialize, Serialize};

use crate::{
    commands::command::Command,
    context::Context,
    network::{NetworkProtocols, ProtocolResponse, SessionManager},
    services::{pending_storage_service::PendingStorageService, publish_service::PublishService},
    types::{
        models::OperationId,
        protocol::StoreResponseData,
        traits::command::{CommandData, CommandExecutionResult, CommandHandler},
    },
};

#[derive(Serialize, Deserialize, Clone)]
pub struct HandlePublishRequestCommandData {
    blockchain: BlockchainName,
    operation_id: OperationId,
    dataset_root: String,
    remote_peer_id: String,
}

impl CommandData for HandlePublishRequestCommandData {
    const COMMAND_NAME: &'static str = "handlePublishRequestCommand";
}

impl HandlePublishRequestCommandData {
    pub fn new(
        blockchain: BlockchainName,
        operation_id: OperationId,
        dataset_root: String,
        remote_peer_id: String,
    ) -> Self {
        Self {
            blockchain,
            operation_id,
            dataset_root,
            remote_peer_id,
        }
    }
}

pub struct HandlePublishRequestCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    publish_service: Arc<PublishService>,
    pending_storage_service: Arc<PendingStorageService>,
    session_manager: Arc<SessionManager<StoreResponseData>>,
}

impl HandlePublishRequestCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            publish_service: Arc::clone(context.publish_service()),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
            session_manager: Arc::clone(context.store_session_manager()),
        }
    }
}

#[async_trait]
impl CommandHandler for HandlePublishRequestCommandHandler {
    fn name(&self) -> &'static str {
        HandlePublishRequestCommandData::COMMAND_NAME
    }

    async fn execute(&self, command: &Command) -> CommandExecutionResult {
        let data = HandlePublishRequestCommandData::from_command(command);

        let HandlePublishRequestCommandData {
            blockchain,
            operation_id,
            dataset_root,
            remote_peer_id,
        } = data;

        /*  // Retrieve the cached channel
        let channel = match self
            .session_manager
            .retrieve_channel(&remote_peer_id, operation_id)
        {
            Some(channel) => channel,
            None => {
                tracing::error!(
                    "No cached session found for peer: {}, operation_id: {}. Session may have expired.",
                    remote_peer_id,
                    operation_id
                );
                return CommandExecutionResult::Completed;
            }
        };

        */

        CommandExecutionResult::Completed
    }
}
