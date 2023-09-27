use crate::context::Context;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use repository::models::commands;
use serde_json::Value;
use std::{fmt, str::FromStr, sync::Arc};
use uuid::Uuid;

use super::find_nodes_command::ProtocolOperation;

#[derive(Debug, Clone, PartialEq)]
pub enum CommandName {
    Default,
    DialPeers,
    FindNodes,
}

impl fmt::Display for CommandName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CommandName::DialPeers => write!(f, "dialPeersCommand"),
            CommandName::FindNodes => write!(f, "findNodesCommand"),
            CommandName::Default => panic!("Should never use default name"),
        }
    }
}

impl FromStr for CommandName {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "dialPeersCommand" => Ok(CommandName::DialPeers),
            "findNodesCommand" => Ok(CommandName::FindNodes),
            _ => Err(()),
        }
    }
}

pub enum CommandResult {
    Completed,
    Repeat,
    Retry,
}

#[derive(Debug, Clone)]
pub enum CommandStatus {
    Failed,
    Expired,
    Started,
    Pending,
    Completed,
    Repeating,
}

impl fmt::Display for CommandStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match *self {
            CommandStatus::Failed => "FAILED",
            CommandStatus::Expired => "EXPIRED",
            CommandStatus::Started => "STARTED",
            CommandStatus::Pending => "PENDING",
            CommandStatus::Completed => "COMPLETED",
            CommandStatus::Repeating => "REPEATING",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for CommandStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "FAILED" => Ok(CommandStatus::Failed),
            "EXPIRED" => Ok(CommandStatus::Expired),
            "STARTED" => Ok(CommandStatus::Started),
            "PENDING" => Ok(CommandStatus::Pending),
            "COMPLETED" => Ok(CommandStatus::Completed),
            "REPEATING" => Ok(CommandStatus::Repeating),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoreCommand {
    pub id: Uuid,
    pub name: CommandName,
    pub sequence: Option<Value>,
    pub ready_at: i64,
    pub delay: i64,
    pub started_at: Option<i64>,
    pub deadline_at: Option<i64>,
    pub period: Option<i64>,
    pub status: CommandStatus,
    pub message: Option<String>,
    pub parent_id: Option<Uuid>,
    pub retries: i32,
    pub transactional: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CoreCommand {
    pub fn from_model(model: commands::Model) -> Self {
        Self {
            id: uuid::Uuid::from_str(model.id.as_str()).unwrap(),
            name: model.name.parse().unwrap(),
            sequence: Some(model.sequence),
            ready_at: model.ready_at,
            delay: model.delay,
            started_at: model.started_at,
            deadline_at: model.deadline_at,
            period: model.period,
            status: model.status.parse().unwrap(),
            message: model.message,
            parent_id: model
                .parent_id
                .map(|string| uuid::Uuid::from_str(string.as_str()).unwrap()),
            retries: model.retries,
            transactional: model.transactional,
            created_at: model.created_at,
            updated_at: model.updated_at,
        }
    }
}

impl Default for CoreCommand {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name: CommandName::Default,
            sequence: None,
            ready_at: now.timestamp_millis(),
            delay: 0,
            started_at: None,
            deadline_at: None,
            period: None,
            status: CommandStatus::Pending,
            message: None,
            parent_id: None,
            retries: 0,
            transactional: false,
            created_at: now,
            updated_at: now,
        }
    }
}

#[async_trait]
pub trait CommandTrait: Send + Sync {
    async fn execute(&self, context: Arc<Context>) -> CommandResult;

    async fn recover(&self) -> CommandResult {
        self.handle_error().await
    }

    async fn handle_error(&self) -> CommandResult {
        // TODO: add error handling
        tracing::error!("Command error (): ");

        CommandResult::Completed
    }

    async fn retry_finished(&self) {
        tracing::trace!("Max retry count for command reached!");
    }

    fn get_core(&self) -> &CoreCommand;

    fn get_json_data(&self) -> Value;

    fn to_command(&self) -> commands::Model {
        let core = self.get_core();
        commands::Model {
            id: core.id.hyphenated().to_string(),
            name: core.name.to_string(),
            data: serde_json::to_value(self.get_json_data())
                .expect("Failed to convert command data to Value"),
            sequence: serde_json::to_value(&core.sequence)
                .expect("Failed to convert command sequence to Value"),
            ready_at: core.ready_at,
            delay: core.delay,
            started_at: core.started_at,
            deadline_at: core.deadline_at,
            period: core.period,
            status: core.status.to_string(),
            message: core.message.clone(),
            parent_id: core.parent_id.map(|uuid| uuid.hyphenated().to_string()),
            transactional: core.transactional,
            retries: core.retries,
            created_at: core.created_at,
            updated_at: core.updated_at,
        }
    }
}
