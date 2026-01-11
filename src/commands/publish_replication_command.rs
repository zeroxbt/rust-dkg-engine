use super::command_handler::{CommandExecutionResult, CommandHandler};
use crate::services::operation_service::OperationId;
use crate::{commands::command::Command, context::Context};
use async_trait::async_trait;
use blockchain::BlockchainName;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/* {
    datasetRoot,
    blockchain,
    operationId,
    storeType: LOCAL_STORE_TYPES.TRIPLE,
    minimumNumberOfNodeReplications,
} */

#[derive(Serialize, Deserialize, Clone)]
pub struct PublishReplicationCommandData {
    operation_id: OperationId,
    blockchain: BlockchainName,
    dataset_root: String,
    minimum_number_of_node_replications: u8,
}

impl PublishReplicationCommandData {
    pub fn new(
        operation_id: OperationId,
        blockchain: BlockchainName,
        dataset_root: String,
        minimum_number_of_node_replications: u8,
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
    async fn execute(&self, data: &Command) -> CommandExecutionResult {
        CommandExecutionResult::Completed
    }
}
