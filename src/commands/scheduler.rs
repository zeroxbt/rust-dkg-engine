use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::constants::{COMMAND_QUEUE_SIZE, MAX_COMMAND_DELAY};
use crate::commands::executor::CommandExecutionRequest;

/// Handle for scheduling commands. Can be cloned and shared across the application.
#[derive(Clone)]
pub(crate) struct CommandScheduler {
    tx: mpsc::Sender<CommandExecutionRequest>,
    shutdown: CancellationToken,
}

impl CommandScheduler {
    /// Create a new command scheduler channel pair.
    /// Returns the scheduler (for sending commands) and a receiver (for the executor).
    pub(crate) fn channel() -> (Self, mpsc::Receiver<CommandExecutionRequest>) {
        let (tx, rx) = mpsc::channel::<CommandExecutionRequest>(COMMAND_QUEUE_SIZE);
        let shutdown = CancellationToken::new();
        (Self { tx, shutdown }, rx)
    }

    /// Signal shutdown to stop accepting new commands.
    /// Spawned delay tasks will check this before sending.
    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Returns a future that completes when shutdown is requested.
    pub(crate) fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
        self.shutdown.cancelled()
    }

    /// Schedule a command for execution. Logs an error if scheduling fails.
    pub(crate) async fn schedule(&self, request: CommandExecutionRequest) {
        // Don't schedule new commands if shutdown has been signaled
        if self.shutdown.is_cancelled() {
            tracing::debug!(
                command = %request.command().name(),
                "Shutdown in progress, not scheduling command"
            );
            return;
        }

        let command_name = request.command().name();
        let delay = request.delay().min(MAX_COMMAND_DELAY);

        let result = if delay > Duration::ZERO {
            let tx = self.tx.clone();
            let shutdown = self.shutdown.clone();
            let mut delayed_request = request;
            delayed_request.clear_delay();

            tokio::spawn(async move {
                tokio::time::sleep(delay).await;

                // Check shutdown before sending - this is the key fix
                if shutdown.is_cancelled() {
                    tracing::debug!(
                        command = %command_name,
                        "Shutdown in progress, dropping delayed command"
                    );
                    return;
                }

                if let Err(e) = tx.send(delayed_request).await {
                    tracing::error!(
                        command = %command_name,
                        error = %e,
                        "Failed to schedule delayed command"
                    );
                }
            });

            return;
        } else {
            self.tx.send(request).await
        };

        if let Err(e) = result {
            tracing::error!(
                command = %command_name,
                error = %e,
                "Failed to schedule command"
            );
        }
    }
}
