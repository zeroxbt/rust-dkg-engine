use super::command::CommandName;
use crate::commands::command::{CommandResult, CommandTrait, CoreCommand};
use crate::constants::DIAL_PEERS_COMMAND_FREQUENCY_MILLS;
use crate::context::Context;
use async_trait::async_trait;
use repository::models::commands;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone)]
pub struct DialPeersCommand {
    core: CoreCommand,
    data: DialPeersCommandData,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DialPeersCommandData;

#[async_trait]
impl CommandTrait for DialPeersCommand {
    async fn execute(&self, context: Arc<Context>) -> CommandResult {
        tracing::info!("Started executing dial peers command...");

        tracing::info!("Finished executing dial peers command...");

        // TODO: get peers from sharding table and dial them

        CommandResult::Completed
    }

    async fn recover(&self) -> CommandResult {
        tracing::warn!("Failed to dial peers: error: ");

        CommandResult::Repeat
    }

    fn get_core(&self) -> &CoreCommand {
        &self.core
    }

    fn get_json_data(&self) -> Value {
        serde_json::to_value(&self.data).unwrap()
    }
}

impl Default for DialPeersCommand {
    fn default() -> Self {
        Self {
            core: CoreCommand {
                name: CommandName::DialPeers,
                period: Some(DIAL_PEERS_COMMAND_FREQUENCY_MILLS),
                ..CoreCommand::default()
            },
            data: DialPeersCommandData {},
        }
    }
}

impl From<commands::Model> for DialPeersCommand {
    fn from(model: commands::Model) -> Self {
        Self {
            core: CoreCommand::from_model(model.clone()),
            data: serde_json::from_value(model.data).unwrap(),
        }
    }
}
