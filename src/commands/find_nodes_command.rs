use super::command::CommandName;
use crate::commands::command::{AbstractCommand, CommandExecutionResult, CoreCommand};
use crate::context::Context;
use async_trait::async_trait;
use blockchain::BlockchainName;
use repository::models::command;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct FindNodesCommand {
    core: CoreCommand,
    data: FindNodesCommandData,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProtocolOperation {
    Publish,
    Get,
    Update,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FindNodesCommandData {
    operation_id: Uuid,
    keyword: String,
    blockchain: BlockchainName,
    operation: ProtocolOperation,
    hash_function_id: i32,
}

#[async_trait]
impl AbstractCommand for FindNodesCommand {
    async fn execute(&self, context: &Arc<Context>) -> CommandExecutionResult {
        tracing::info!(
            "Searching for closest nodes for keyword: {}",
            self.data.keyword
        );

        // TODO: implement this

        tracing::info!("Found ${} node(s) for keyword ${}", 1, self.data.keyword);

        CommandExecutionResult::Completed
    }

    fn core(&self) -> &CoreCommand {
        &self.core
    }

    fn json_data(&self) -> Value {
        serde_json::to_value(&self.data).unwrap()
    }
}

impl FindNodesCommand {
    pub fn new(
        operation_id: Uuid,
        keyword: String,
        blockchain: BlockchainName,
        operation: ProtocolOperation,
        hash_function_id: i32,
    ) -> Self {
        Self {
            core: CoreCommand {
                name: CommandName::FindNodes,
                ..CoreCommand::default()
            },
            data: FindNodesCommandData {
                operation_id,
                keyword,
                blockchain,
                operation,
                hash_function_id,
            },
        }
    }
}

impl From<command::Model> for FindNodesCommand {
    fn from(model: command::Model) -> Self {
        Self {
            core: CoreCommand::from_model(model.clone()),
            data: serde_json::from_value(model.data).unwrap(),
        }
    }
}
