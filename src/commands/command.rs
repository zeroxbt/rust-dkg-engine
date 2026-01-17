use async_trait::async_trait;

use crate::commands::command_executor::CommandExecutionResult;

// Note: Must use async-trait here because this trait is used with trait objects (Arc<dyn
// CommandHandler>) Native async traits are not dyn-compatible yet
#[async_trait]
pub trait CommandHandler<D: Send + Sync + 'static>: Send + Sync {
    async fn execute(&self, data: &D) -> CommandExecutionResult;
}
