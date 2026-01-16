use async_trait::async_trait;

use crate::commands::command::Command;

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

    /// Called when a command returns `Retry` but has exhausted all retries.
    async fn retry_finished(&self) {
        tracing::trace!("Max retry count for command reached!");
    }
}
