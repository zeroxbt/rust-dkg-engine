use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use metrics::{counter, histogram};
use tokio::sync::{Mutex, Semaphore, mpsc};

use super::{
    command_registry::{Command, CommandResolver},
    constants::{COMMAND_QUEUE_PARALLELISM, MAX_COMMAND_DELAY, MAX_COMMAND_LIFETIME},
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
}

impl CommandScheduler {
    /// Create a new command scheduler channel pair.
    /// Returns the scheduler (for sending commands) and a receiver (for the executor).
    pub(crate) fn channel() -> (Self, mpsc::Receiver<CommandExecutionRequest>) {
        let (tx, rx) = mpsc::channel::<CommandExecutionRequest>(COMMAND_QUEUE_PARALLELISM);
        (Self { tx }, rx)
    }

    /// Schedule a command for execution. Logs an error if scheduling fails.
    pub(crate) async fn schedule(&self, request: CommandExecutionRequest) {
        let command_name = request.command().name();
        let delay = request.delay().min(MAX_COMMAND_DELAY);

        counter!("command_executor_scheduled_total", "command" => command_name).increment(1);

        let result = if delay > Duration::ZERO {
            let tx = self.tx.clone();
            let mut delayed_request = request;
            delayed_request.clear_delay();

            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
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

pub(crate) struct CommandExecutor {
    command_resolver: CommandResolver,
    scheduler: CommandScheduler,
    rx: Arc<Mutex<mpsc::Receiver<CommandExecutionRequest>>>,
    semaphore: Arc<Semaphore>,
}

// TODO:
//   - add priority based scheduling
//   - switch to redis based worker queues or something similar
//   - add error handling

impl CommandExecutor {
    pub(crate) fn new(context: Arc<Context>, rx: mpsc::Receiver<CommandExecutionRequest>) -> Self {
        Self {
            command_resolver: CommandResolver::new(Arc::clone(&context)),
            scheduler: context.command_scheduler().clone(),
            rx: Arc::new(Mutex::new(rx)),
            semaphore: Arc::new(Semaphore::new(COMMAND_QUEUE_PARALLELISM)),
        }
    }

    /// Schedule initial commands to be executed.
    pub(crate) async fn schedule_commands(&self, requests: Vec<CommandExecutionRequest>) {
        for request in requests {
            self.scheduler.schedule(request).await;
        }
    }

    pub(crate) async fn run(&self) {
        let mut pending_tasks = FuturesUnordered::new();

        loop {
            let mut locked_rx = self.rx.lock().await;

            tokio::select! {
                _ = pending_tasks.select_next_some(), if !pending_tasks.is_empty() => {
                    // Continue the loop when a task completes.
                }
                command = locked_rx.recv() => {
                    match command {
                        Some(request) => {
                            drop(locked_rx);
                            let permit = self.semaphore.clone().acquire_owned().await.expect("Permit aquired");
                            pending_tasks.push(self.execute(request, permit));
                        }
                        None => {
                            tracing::info!("Command channel closed, shutting down executor");
                            break;
                        }
                    }
                }
            }
        }

        // Drain pending tasks before returning
        if !pending_tasks.is_empty() {
            tracing::info!(
                pending_count = pending_tasks.len(),
                "Waiting for pending commands to complete"
            );
            while pending_tasks.next().await.is_some() {}
            tracing::info!("All pending commands completed");
        }
    }

    async fn execute(
        &self,
        request: CommandExecutionRequest,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let command_name = request.command().name();

        // Check if command has expired
        if request.is_expired() {
            counter!("command_executor_executed_total", "command" => command_name, "result" => "expired").increment(1);
            tracing::warn!("Command {} expired, dropping", command_name);
            return;
        }

        let start = Instant::now();
        let result = self.command_resolver.execute(request.command()).await;
        let duration = start.elapsed();

        histogram!("command_executor_duration_seconds", "command" => command_name)
            .record(duration.as_secs_f64());

        match result {
            CommandExecutionResult::Repeat { delay } => {
                counter!("command_executor_executed_total", "command" => command_name, "result" => "rescheduled").increment(1);
                let new_request =
                    CommandExecutionRequest::new(request.into_command()).with_delay(delay);
                self.scheduler.schedule(new_request).await;
            }
            CommandExecutionResult::Completed => {
                counter!("command_executor_executed_total", "command" => command_name, "result" => "completed").increment(1);
                tracing::trace!("Command {} completed", command_name);
            }
        }
    }
}
