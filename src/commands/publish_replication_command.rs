use async_trait::async_trait;
use super::command::Command;
use crate::types::models::OperationId;
use crate::types::traits::command::{CommandData, CommandExecutionResult};
use crate::{context::Context, types::traits::command::CommandHandler};
use blockchain::BlockchainName;
use network::action::NetworkAction;
use network::NetworkManager;
use repository::RepositoryManager;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

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
    network_manager: Arc<NetworkManager>,
    network_action_tx: mpsc::Sender<NetworkAction>,
}

impl PublishReplicationCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            network_action_tx: context.network_action_tx().clone(),
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
