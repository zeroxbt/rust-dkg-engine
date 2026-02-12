use std::sync::Arc;

use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{
    constants::{COMMAND_CONCURRENT_LIMIT, MAX_COMMAND_LIFETIME},
    registry::{Command, CommandResolver},
};
use crate::context::Context;

/// Outcome of a command execution.
///
/// Extensible for future variants (e.g. `Retry { delay }`, `Failed`, etc.).
pub(crate) enum CommandOutcome {
    Completed,
}

#[derive(Clone)]
pub(crate) struct CommandExecutionRequest {
    command: Command,
    created_at: i64,
}

impl CommandExecutionRequest {
    pub(crate) fn new(command: Command) -> Self {
        Self {
            command,
            created_at: Utc::now().timestamp_millis(),
        }
    }

    /// Returns a reference to the command.
    pub(crate) fn command(&self) -> &Command {
        &self.command
    }

    pub(crate) fn is_expired(&self) -> bool {
        let now = Utc::now().timestamp_millis();
        let elapsed_ms = (now - self.created_at).max(0) as u64;
        std::time::Duration::from_millis(elapsed_ms) > MAX_COMMAND_LIFETIME
    }
}

/// CommandExecutor owns the receiver and runs the command processing loop.
///
/// Processes operation commands received from the channel. Periodic tasks
/// are managed separately and do not flow through this executor.
pub(crate) struct CommandExecutor {
    command_resolver: Arc<CommandResolver>,
    rx: mpsc::Receiver<CommandExecutionRequest>,
    shutdown: CancellationToken,
}

// TODO:
//   - add priority based scheduling
//   - switch to redis based worker queues or something similar
//   - add error handling

impl CommandExecutor {
    pub(crate) fn new(context: Arc<Context>, rx: mpsc::Receiver<CommandExecutionRequest>) -> Self {
        let shutdown = context.command_scheduler().shutdown_token();
        Self {
            command_resolver: Arc::new(CommandResolver::new(context)),
            rx,
            shutdown,
        }
    }

    /// Runs the command executor loop until the channel is closed.
    ///
    /// This method consumes self and runs until the command channel is closed
    /// (when all senders are dropped). Pending commands are allowed to complete
    /// before the executor exits.
    pub(crate) async fn run(mut self) {
        let mut pending_tasks: FuturesUnordered<_> = FuturesUnordered::new();
        let mut intake_closed = false;
        let mut shutdown_logged = false;

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled(), if !self.rx.is_closed() => {
                    // Stop accepting new commands, but keep draining already buffered ones.
                    self.rx.close();
                    if !shutdown_logged {
                        tracing::info!("Command executor shutdown signaled; draining queued commands");
                        shutdown_logged = true;
                    }
                }
                _ = pending_tasks.select_next_some(), if !pending_tasks.is_empty() => {
                    // Command completed
                }
                command = self.rx.recv(), if !intake_closed && pending_tasks.len() < COMMAND_CONCURRENT_LIMIT => {
                    match command {
                        Some(request) => {
                            let resolver = Arc::clone(&self.command_resolver);
                            pending_tasks.push(Self::execute(resolver, request));
                        }
                        None => {
                            intake_closed = true;
                            tracing::info!("Command intake closed; waiting for in-flight commands to complete");
                        }
                    }
                }
                else => {
                    if intake_closed && pending_tasks.is_empty() {
                        break;
                    }
                }
            }
        }
    }

    /// Execute a command.
    async fn execute(command_resolver: Arc<CommandResolver>, request: CommandExecutionRequest) {
        let command_name = request.command().name();

        // Check if command has expired
        if request.is_expired() {
            tracing::warn!(command = %command_name, "Command expired, dropping");
            return;
        }

        let _outcome = command_resolver.execute(request.command()).await;
        tracing::trace!(command = %command_name, "Command completed");
    }
}
