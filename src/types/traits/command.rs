use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

use crate::commands::command::{Command, CommandBuilder};

pub enum CommandExecutionResult {
    Completed,
    Repeat,
    Retry,
}

#[derive(Clone, Debug)]
pub enum ScheduleConfig {
    OneShot,
    Periodic {
        period_ms: i64,
        initial_delay_ms: i64,
    },
}

impl ScheduleConfig {
    pub fn periodic(period_ms: i64) -> Self {
        Self::Periodic {
            period_ms,
            initial_delay_ms: 0,
        }
    }

    pub fn periodic_with_delay(period_ms: i64, initial_delay_ms: i64) -> Self {
        Self::Periodic {
            period_ms,
            initial_delay_ms,
        }
    }
}

// Note: Must use async-trait here because this trait is used with trait objects (Arc<dyn
// CommandHandler>) Native async traits are not dyn-compatible yet
#[async_trait]
pub trait CommandHandler: Send + Sync {
    fn name(&self) -> &'static str;

    fn schedule_config(&self) -> ScheduleConfig {
        ScheduleConfig::OneShot
    }

    async fn execute(&self, command: &Command) -> CommandExecutionResult;

    async fn recover(&self) -> CommandExecutionResult {
        self.handle_error().await
    }

    async fn handle_error(&self) -> CommandExecutionResult {
        // TODO: add error handling
        tracing::error!("Command error (): ");

        CommandExecutionResult::Completed
    }

    async fn retry_finished(&self) {
        tracing::trace!("Max retry count for command reached!");
    }
}

pub trait CommandData: Serialize + DeserializeOwned + Sized {
    const COMMAND_NAME: &'static str;

    fn from_command(command: &Command) -> Self {
        serde_json::from_value(command.data.clone())
            .unwrap_or_else(|e| panic!("Invalid command data for {}: {}", Self::COMMAND_NAME, e))
    }

    fn into_command(self) -> Command {
        Command::builder(Self::COMMAND_NAME).data(self).build()
    }

    fn to_command_builder(self) -> CommandBuilder {
        Command::builder(Self::COMMAND_NAME).data(self)
    }
}
