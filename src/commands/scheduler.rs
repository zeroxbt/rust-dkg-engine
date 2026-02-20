use dkg_observability as observability;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::constants::COMMAND_QUEUE_SIZE;
use crate::commands::executor::CommandExecutionRequest;

/// Handle for scheduling commands. Can be cloned and shared across the application.
///
/// This is used by external callers (RPC controllers, HTTP API, periodic tasks) to submit
/// operation commands to the executor.
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
    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Get a clone of the scheduler shutdown token.
    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Schedule a command for execution.
    ///
    /// Sends the command to the executor's channel. Blocks if the channel is full
    /// (backpressure). Returns immediately if shutdown has been signaled.
    ///
    /// Use this for commands that must not be dropped (internal scheduling).
    /// For inbound peer requests, prefer [`try_schedule`] to avoid blocking the
    /// network event loop.
    pub(crate) async fn schedule(&self, request: CommandExecutionRequest) {
        if self.shutdown.is_cancelled() {
            observability::record_command_total(request.command().name(), "rejected_shutdown");
            tracing::debug!(
                command = %request.command().name(),
                "Shutdown in progress, not scheduling command"
            );
            return;
        }

        let command_name = request.command().name();

        if let Err(e) = self.tx.send(request).await {
            observability::record_command_total(command_name, "rejected_channel_closed");
            tracing::error!(
                command = %command_name,
                error = %e,
                "Failed to schedule command"
            );
        }
    }

    /// Try to schedule a command without blocking.
    ///
    /// Returns `true` if the command was accepted, `false` if the channel is full
    /// or shutdown is in progress. Callers should respond with Busy when this
    /// returns `false`.
    pub(crate) fn try_schedule(&self, request: CommandExecutionRequest) -> bool {
        if self.shutdown.is_cancelled() {
            observability::record_command_total(request.command().name(), "rejected_shutdown");
            tracing::debug!(
                command = %request.command().name(),
                "Shutdown in progress, not scheduling command"
            );
            return false;
        }

        match self.tx.try_send(request) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(request)) => {
                observability::record_command_total(
                    request.command().name(),
                    "rejected_queue_full",
                );
                tracing::warn!("Command queue full, rejecting command");
                false
            }
            Err(mpsc::error::TrySendError::Closed(request)) => {
                observability::record_command_total(
                    request.command().name(),
                    "rejected_channel_closed",
                );
                tracing::error!("Command channel closed");
                false
            }
        }
    }
}
