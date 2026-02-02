use std::{sync::Arc, time::Duration};

use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    command_registry::{Command, CommandResolver},
    constants::{
        COMMAND_CONCURRENT_LIMIT, COMMAND_QUEUE_SIZE, MAX_COMMAND_DELAY, MAX_COMMAND_LIFETIME,
    },
};
use crate::context::Context;

pub(crate) enum CommandExecutionResult {
    Completed,
    Repeat { delay: Duration },
}

#[derive(Clone)]
pub(crate) struct CommandExecutionRequest {
    command: Command,
    delay: Duration,
    created_at: i64,
}

impl CommandExecutionRequest {
    pub(crate) fn new(command: Command) -> Self {
        Self {
            command,
            delay: Duration::ZERO,
            created_at: Utc::now().timestamp_millis(),
        }
    }

    pub(crate) fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Returns a reference to the command.
    pub(crate) fn command(&self) -> &Command {
        &self.command
    }

    /// Consumes the request and returns the command.
    pub(crate) fn into_command(self) -> Command {
        self.command
    }

    /// Returns the delay duration.
    pub(crate) fn delay(&self) -> Duration {
        self.delay
    }

    /// Sets the delay to zero.
    pub(crate) fn clear_delay(&mut self) {
        self.delay = Duration::ZERO;
    }

    pub(crate) fn is_expired(&self) -> bool {
        let now = Utc::now().timestamp_millis();
        let elapsed_ms = (now - self.created_at).max(0) as u64;
        Duration::from_millis(elapsed_ms) > MAX_COMMAND_LIFETIME
    }
}

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

/// CommandExecutor owns the receiver and runs the command processing loop.
///
/// This follows the same pattern as NetworkEventLoop - it owns the receiver
/// and is consumed when run() is called. The scheduler is passed by reference
/// to avoid circular dependencies.
pub(crate) struct CommandExecutor {
    command_resolver: Arc<CommandResolver>,
    rx: mpsc::Receiver<CommandExecutionRequest>,
    semaphore: Arc<Semaphore>,
}

// TODO:
//   - add priority based scheduling
//   - switch to redis based worker queues or something similar
//   - add error handling

impl CommandExecutor {
    pub(crate) fn new(context: Arc<Context>, rx: mpsc::Receiver<CommandExecutionRequest>) -> Self {
        Self {
            command_resolver: Arc::new(CommandResolver::new(context)),
            rx,
            semaphore: Arc::new(Semaphore::new(COMMAND_CONCURRENT_LIMIT)),
        }
    }

    /// Runs the command executor loop until shutdown.
    ///
    /// This method consumes self and runs until either:
    /// - Shutdown is requested and no pending commands remain
    /// - The command channel is closed
    pub(crate) async fn run(mut self, scheduler: &CommandScheduler) {
        let mut pending_tasks: FuturesUnordered<_> = FuturesUnordered::new();

        loop {
            tokio::select! {
                // Graceful shutdown: exit when shutdown is requested and no pending work
                _ = scheduler.cancelled(), if pending_tasks.is_empty() => {
                    tracing::info!("Shutdown requested and no pending commands, exiting executor");
                    break;
                }
                result = pending_tasks.select_next_some(), if !pending_tasks.is_empty() => {
                    // Reschedule command if it returned Repeat
                    if let Some(request) = result {
                        scheduler.schedule(request).await;
                    }
                }
                command = self.rx.recv() => {
                    match command {
                        Some(request) => {
                            let permit = self.semaphore.clone().acquire_owned().await.expect("Permit acquired");
                            let resolver = Arc::clone(&self.command_resolver);
                            pending_tasks.push(Self::execute(resolver, request, permit));
                        }
                        None => {
                            tracing::info!("Command channel closed, shutting down executor");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Execute a command and return a reschedule request if the command wants to repeat.
    async fn execute(
        command_resolver: Arc<CommandResolver>,
        request: CommandExecutionRequest,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Option<CommandExecutionRequest> {
        let command_name = request.command().name();

        // Check if command has expired
        if request.is_expired() {
            tracing::warn!(command = %command_name, "Command expired, dropping");
            return None;
        }

        let result = command_resolver.execute(request.command()).await;

        match result {
            CommandExecutionResult::Repeat { delay } => {
                Some(CommandExecutionRequest::new(request.into_command()).with_delay(delay))
            }
            CommandExecutionResult::Completed => {
                tracing::trace!(command = %command_name, "Command completed");
                None
            }
        }
    }
}
