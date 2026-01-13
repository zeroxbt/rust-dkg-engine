use std::sync::Arc;

use async_trait::async_trait;
use blockchain::BlockchainName;
use network::NetworkManager;
use repository::RepositoryManager;
use serde::{Deserialize, Serialize};

use super::command::Command;
use crate::{
    context::Context,
    network::NetworkProtocols,
    types::{
        models::OperationId,
        traits::command::{CommandData, CommandExecutionResult, CommandHandler},
    },
};

#[derive(Serialize, Deserialize, Clone)]
pub struct PublishReplicationCommandData {
    operation_id: OperationId,
    blockchain: BlockchainName,
    dataset_root: String,
    minimum_number_of_node_replications: Option<u8>,
}

impl CommandData for PublishReplicationCommandData {
    const COMMAND_NAME: &'static str = "publishReplicationCommand";
}

impl PublishReplicationCommandData {
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

pub struct PublishReplicationCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
}

impl PublishReplicationCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
        }
    }
}

#[async_trait]
impl CommandHandler for PublishReplicationCommandHandler {
    fn name(&self) -> &'static str {
        PublishReplicationCommandData::COMMAND_NAME
    }

    async fn execute(&self, command: &Command) -> CommandExecutionResult {
        let _data: PublishReplicationCommandData = serde_json::from_value(command.data.clone())
            .expect("Invalid command data for publishReplicationCommand");

        // TODO: Implement publish replication logic

        CommandExecutionResult::Completed
    }
}
