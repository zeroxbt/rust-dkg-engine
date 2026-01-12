use chrono::{DateTime, Utc};
use repository::models::command::{self, Model};
use serde_json::Value;
use std::{fmt, str::FromStr};
use uuid::Uuid;

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

#[derive(Clone)]
pub struct Command {
    pub id: Uuid,
    pub name: String,
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
    pub data: serde_json::Value,
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
    pub fn new(name: String, data: serde_json::Value, retries: i32, period: Option<i64>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            data,
            ready_at: now.timestamp_millis(),
            delay: 0,
            started_at: None,
            deadline_at: None,
            period,
            status: CommandStatus::Pending,
            message: None,
            parent_id: None,
            retries,
            sequence: None,
            transactional: false,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn builder(name: impl Into<String>) -> CommandBuilder {
        CommandBuilder::new(name.into())
    }
}

pub struct CommandBuilder {
    name: String,
    data: Option<Value>,
    delay: i64,
    retries: i32,
    period: Option<i64>,
    deadline_at: Option<i64>,
    sequence: Option<Value>,
    transactional: bool,
}

impl CommandBuilder {
    pub fn new(name: String) -> Self {
        Self {
            name,
            data: None,
            delay: 0,
            retries: 0,
            period: None,
            deadline_at: None,
            sequence: None,
            transactional: false,
        }
    }

    pub fn data<T: serde::Serialize>(mut self, data: T) -> Self {
        self.data = Some(serde_json::to_value(data).expect("Failed to serialize data"));
        self
    }

    pub fn data_json(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    pub fn delay(mut self, delay: i64) -> Self {
        self.delay = delay;
        self
    }

    pub fn retries(mut self, retries: i32) -> Self {
        self.retries = retries;
        self
    }

    pub fn period(mut self, period: i64) -> Self {
        self.period = Some(period);
        self
    }

    pub fn deadline_at(mut self, deadline_at: i64) -> Self {
        self.deadline_at = Some(deadline_at);
        self
    }

    pub fn sequence(mut self, sequence: Value) -> Self {
        self.sequence = Some(sequence);
        self
    }

    pub fn transactional(mut self, transactional: bool) -> Self {
        self.transactional = transactional;
        self
    }

    pub fn build(self) -> Command {
        let now = Utc::now();
        Command {
            id: Uuid::new_v4(),
            name: self.name,
            data: self.data.unwrap_or(serde_json::json!({})),
            ready_at: now.timestamp_millis(),
            delay: self.delay,
            started_at: None,
            deadline_at: self.deadline_at,
            period: self.period,
            status: CommandStatus::Pending,
            message: None,
            parent_id: None,
            retries: self.retries,
            sequence: self.sequence,
            transactional: self.transactional,
            created_at: now,
            updated_at: now,
        }
    }
}

impl Command {
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
