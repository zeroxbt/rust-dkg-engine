use super::command::CommandName;
use crate::commands::command::{CommandResult, CommandTrait, CoreCommand};
use crate::context::Context;
use async_trait::async_trait;
use repository::models::commands;
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
    blockchain: String,
    operation: ProtocolOperation,
    hash_function_id: i32,
}

#[async_trait]
impl CommandTrait for FindNodesCommand {
    async fn execute(&self, context: Arc<Context>) -> CommandResult {
        tracing::info!(
            "Searching for closest nodes for keyword: {}",
            self.data.keyword
        );

        /*  let one_peer_id = "QmTT3iPYPjLxy7N8Z7u8sK6wwvfg2cMDV3NVxuW1WT23XY"
        .parse()
        .unwrap(); */

        tracing::info!("Found ${} node(s) for keyword ${}", 1, self.data.keyword);

        CommandResult::Completed
    }

    fn get_core(&self) -> &CoreCommand {
        &self.core
    }

    fn get_json_data(&self) -> Value {
        serde_json::to_value(&self.data).unwrap()
    }
}

impl FindNodesCommand {
    pub fn new(
        operation_id: Uuid,
        keyword: String,
        blockchain: String,
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

impl From<commands::Model> for FindNodesCommand {
    fn from(model: commands::Model) -> Self {
        Self {
            core: CoreCommand::from_model(model.clone()),
            data: serde_json::from_value(model.data).unwrap(),
        }
    }
}
