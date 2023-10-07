use super::command::{Command, CommandData};
use super::command_handler::{CommandExecutionResult, CommandHandler};
use crate::context::Context;
use crate::services::publish_service::ProtocolOperation;
use async_trait::async_trait;
use blockchain::BlockchainName;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone)]
pub struct FindNodesCommandData {
    operation_id: Uuid,
    keyword: String,
    blockchain: BlockchainName,
    operation: ProtocolOperation,
    hash_function_id: i32,
}

impl FindNodesCommandData {
    pub fn new(
        operation_id: Uuid,
        keyword: String,
        blockchain: BlockchainName,
        operation: ProtocolOperation,
        hash_function_id: i32,
    ) -> Self {
        Self {
            operation_id,
            keyword,
            blockchain,
            operation,
            hash_function_id,
        }
    }
}

pub struct FindNodesCommandHandler {
    context: Arc<Context>,
}

impl FindNodesCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self { context }
    }
}

#[async_trait]
impl CommandHandler for FindNodesCommandHandler {
    async fn execute(&self, command: &Command) -> CommandExecutionResult {
        let data = match &command.data {
            CommandData::FindNodes(data) => data,
            _ => panic!("Unable to handle command data."),
        };

        tracing::info!("Searching for closest nodes for keyword: {}", data.keyword);

        let peers = self
            .context
            .repository_manager()
            .shard_repository()
            .get_all_peer_records(data.blockchain.as_str(), true)
            .await
            .unwrap();

        tracing::info!(
            "Found ${} node(s) for keyword ${}",
            peers.len(),
            data.keyword
        );

        CommandExecutionResult::Completed
    }
}
