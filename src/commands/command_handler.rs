use super::command::Command;
use async_trait::async_trait;

pub enum CommandExecutionResult {
    Completed,
    Repeat,
    Retry,
}

#[async_trait]
pub trait CommandHandler: Send + Sync {
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
