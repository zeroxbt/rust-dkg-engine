use std::sync::Arc;

use async_trait::async_trait;
use blockchain::{
    BlockchainManager, BlockchainName, H256, Token, U256,
    utils::{SignatureComponents, keccak256_encode_packed},
};
use futures::future::join_all;
use libp2p::PeerId;
use network::{
    NetworkManager, RequestMessage, ResponseMessage,
    message::{
        RequestMessageHeader, RequestMessageType, ResponseMessageHeader, ResponseMessageType,
    },
};
use repository::RepositoryManager;
use serde::{Deserialize, Serialize};

use crate::{
    commands::command::Command,
    context::Context,
    network::{NetworkProtocols, ProtocolRequest},
    services::{pending_storage_service::PendingStorageService, publish_service::PublishService},
    types::{
        models::OperationId,
        protocol::{StoreRequestData, StoreResponseData},
        traits::command::{CommandData, CommandExecutionResult, CommandHandler},
    },
};

#[derive(Serialize, Deserialize, Clone)]
pub struct SendPublishRequestsCommandData {
    operation_id: OperationId,
    blockchain: BlockchainName,
    dataset_root: String,
    minimum_number_of_node_replications: Option<u8>,
}

impl CommandData for SendPublishRequestsCommandData {
    const COMMAND_NAME: &'static str = "sendPublishRequestsCommand";
}

impl SendPublishRequestsCommandData {
    pub fn new(
        operation_id: OperationId,
        blockchain: BlockchainName,
        dataset_root: String,
        minimum_number_of_node_replications: Option<u8>,
    ) -> Self {
        Self {
            operation_id,
            blockchain,
            dataset_root,
            minimum_number_of_node_replications,
        }
    }
}

pub struct SendPublishRequestsCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    blockchain_manager: Arc<BlockchainManager>,
    pending_storage_service: Arc<PendingStorageService>,
    publish_service: Arc<PublishService>,
}

impl SendPublishRequestsCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            publish_service: Arc::clone(context.publish_service()),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
        }
    }
}

#[async_trait]
impl CommandHandler for SendPublishRequestsCommandHandler {
    fn name(&self) -> &'static str {
        SendPublishRequestsCommandData::COMMAND_NAME
    }

    async fn execute(&self, command: &Command) -> CommandExecutionResult {
        let data = SendPublishRequestsCommandData::from_command(command);

        let SendPublishRequestsCommandData {
            operation_id,
            blockchain,
            dataset_root,
            minimum_number_of_node_replications,
        } = data;

        CommandExecutionResult::Completed
    }
}
