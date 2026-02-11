use std::{sync::Arc, time::Duration};

use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{Semaphore, mpsc};
use tokio_util::time::DelayQueue;

use super::{
    constants::{
        GENERAL_COMMAND_CONCURRENT_LIMIT, MAX_COMMAND_LIFETIME,
        PERIODIC_COMMAND_CONCURRENT_LIMIT,
    },
    registry::{Command, CommandResolver},
};
use crate::{commands::scheduler::CommandScheduler, context::Context};

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
        // Set created_at to the time the command becomes eligible to run.
        // This prevents delayed commands from expiring before their delay elapses.
        let delay_ms = delay.as_millis().min(i64::MAX as u128) as i64;
        self.created_at = Utc::now().timestamp_millis().saturating_add(delay_ms);
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

/// CommandExecutor owns the receiver and runs the command processing loop.
///
/// This follows the same pattern as NetworkEventLoop - it owns the receiver
/// and is consumed when run() is called. The scheduler is passed by reference
/// to avoid circular dependencies.
pub(crate) struct CommandExecutor {
    command_resolver: Arc<CommandResolver>,
    rx: mpsc::Receiver<CommandExecutionRequest>,
    periodic_semaphore: Arc<Semaphore>,
    general_semaphore: Arc<Semaphore>,
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
            periodic_semaphore: Arc::new(Semaphore::new(PERIODIC_COMMAND_CONCURRENT_LIMIT)),
            general_semaphore: Arc::new(Semaphore::new(GENERAL_COMMAND_CONCURRENT_LIMIT)),
        }
    }

    /// Runs the command executor loop until shutdown.
    ///
    /// This method consumes self and runs until either:
    /// - Shutdown is requested and no pending commands remain
    /// - The command channel is closed
    ///
    /// Periodic commands that return `Repeat { delay }` are rescheduled via an
    /// internal `DelayQueue` rather than sending back through the mpsc channel.
    /// This avoids a potential deadlock (executor blocking on its own channel)
    /// and eliminates unbounded spawned delay tasks.
    pub(crate) async fn run(mut self, scheduler: &CommandScheduler) {
        let mut pending_tasks: FuturesUnordered<_> = FuturesUnordered::new();
        let mut delay_queue: DelayQueue<CommandExecutionRequest> = DelayQueue::new();

        loop {
            tokio::select! {
                // Graceful shutdown: exit when shutdown is requested and no pending work
                _ = scheduler.cancelled(), if pending_tasks.is_empty() => {
                    tracing::info!("Shutdown requested and no pending commands, exiting executor");
                    break;
                }
                result = pending_tasks.select_next_some(), if !pending_tasks.is_empty() => {
                    // Reschedule command if it returned Repeat
                    let result: Option<CommandExecutionRequest> = result;
                    if let Some(request) = result {
                        let delay = request.delay();
                        delay_queue.insert(request, delay);
                    }
                }
                // Delayed commands becoming ready for execution
                Some(expired) = delay_queue.next(), if !delay_queue.is_empty() => {
                    let mut request = expired.into_inner();
                    request.clear_delay();
                    let semaphore = if request.command().is_periodic() {
                        &self.periodic_semaphore
                    } else {
                        &self.general_semaphore
                    };
                    let permit = semaphore.clone().acquire_owned().await.expect("Permit acquired");
                    let resolver = Arc::clone(&self.command_resolver);
                    pending_tasks.push(Self::execute(resolver, request, permit));
                }
                // New commands from external callers (RPC controllers, startup, etc.)
                command = self.rx.recv() => {
                    match command {
                        Some(request) => {
                            let delay = request.delay();
                            if delay > Duration::ZERO {
                                delay_queue.insert(request, delay);
                            } else {
                                let semaphore = if request.command().is_periodic() {
                                    &self.periodic_semaphore
                                } else {
                                    &self.general_semaphore
                                };
                                let permit = semaphore.clone().acquire_owned().await.expect("Permit acquired");
                                let resolver = Arc::clone(&self.command_resolver);
                                pending_tasks.push(Self::execute(resolver, request, permit));
                            }
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
