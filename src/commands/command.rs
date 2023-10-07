use chrono::{DateTime, Utc};
use repository::models::command::{self, Model};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, str::FromStr};
use uuid::Uuid;

use super::{
    dial_peers_command::{DialPeersCommandData, DialPeersCommandHandler},
    find_nodes_command::FindNodesCommandData,
};

#[derive(Debug, Clone, PartialEq)]
pub enum CommandName {
    Default,
    DialPeers,
    FindNodes,
}

impl CommandName {
    pub fn to_command(&self) -> Option<Command> {
        match self {
            CommandName::DialPeers => Some(DialPeersCommandHandler::create_default_command()),
            _ => panic!(
                "Default trait not implemented for command with name {}",
                self
            ),
        }
    }
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

#[derive(Serialize, Deserialize, Clone)]
pub enum CommandData {
    FindNodes(FindNodesCommandData),
    DialPeers(DialPeersCommandData),
    Empty,
}

#[derive(Clone)]
pub struct Command {
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
    pub data: CommandData,
}

impl From<Model> for Command {
    fn from(model: command::Model) -> Self {
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
            data: serde_json::from_value(model.data).unwrap(),
        }
    }
}

impl Command {
    pub fn new(name: CommandName, data: CommandData, retries: i32, period: Option<i64>) -> Self {
        Self {
            name,
            period,
            retries,
            data,
            ..Self::default()
        }
    }
    pub fn to_model(&self) -> Model {
        Model {
            id: self.id.hyphenated().to_string(),
            name: self.name.to_string(),
            data: serde_json::to_value(self.data.clone())
                .expect("Failed to convert command data to Value"),
            sequence: serde_json::to_value(&self.sequence)
                .expect("Failed to convert command sequence to Value"),
            ready_at: self.ready_at,
            delay: self.delay,
            started_at: self.started_at,
            deadline_at: self.deadline_at,
            period: self.period,
            status: self.status.to_string(),
            message: self.message.clone(),
            parent_id: self.parent_id.map(|uuid| uuid.hyphenated().to_string()),
            transactional: self.transactional,
            retries: self.retries,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

impl Default for Command {
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
            data: CommandData::Empty,
        }
    }
}
