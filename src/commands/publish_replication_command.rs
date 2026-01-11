use super::command::{Command, CommandData};
use super::command_handler::{CommandExecutionResult, CommandHandler};
use crate::context::Context;
use crate::services::operation_service::OperationId;
use async_trait::async_trait;
use blockchain::BlockchainName;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
    context: Arc<Context>,
}

impl PublishReplicationCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self { context }
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
