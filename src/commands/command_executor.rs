use std::{cmp::min, sync::Arc, time::Duration};

use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{Mutex, Semaphore, mpsc};

use super::{
    command_registry::{Command, CommandResolver},
    constants::{COMMAND_QUEUE_PARALLELISM, MAX_COMMAND_DELAY_MS, MAX_COMMAND_LIFETIME_MS},
};
use crate::context::Context;

pub enum CommandExecutionResult {
    Completed,
    Repeat { delay_ms: i64 },
}

#[derive(Clone)]
pub struct CommandExecutionRequest {
    pub command: Command,
    pub delay_ms: i64,
    pub created_at: i64,
}

impl CommandExecutionRequest {
    pub fn new(command: Command) -> Self {
        Self {
            command,
            delay_ms: 0,
            created_at: Utc::now().timestamp_millis(),
        }
    }

    pub fn with_delay(mut self, delay_ms: i64) -> Self {
        self.delay_ms = delay_ms;
        self
    }

    pub fn is_expired(&self) -> bool {
        let now = Utc::now().timestamp_millis();
        now - self.created_at > MAX_COMMAND_LIFETIME_MS
    }
}

/// Handle for scheduling commands. Can be cloned and shared across the application.
#[derive(Clone)]
pub struct CommandScheduler {
    tx: mpsc::Sender<CommandExecutionRequest>,
}

impl CommandScheduler {
    /// Create a new command scheduler channel pair.
    /// Returns the scheduler (for sending commands) and a receiver (for the executor).
    pub fn channel() -> (Self, mpsc::Receiver<CommandExecutionRequest>) {
        let (tx, rx) = mpsc::channel::<CommandExecutionRequest>(COMMAND_QUEUE_PARALLELISM);
        (Self { tx }, rx)
    }

    /// Schedule a command for execution. Logs an error if scheduling fails.
    pub async fn schedule(&self, request: CommandExecutionRequest) {
        let command_name = request.command.name();
        let delay = min(request.delay_ms.max(0) as u64, MAX_COMMAND_DELAY_MS as u64);

        let result = if delay > 0 {
            let tx = self.tx.clone();
            let mut delayed_request = request;
            delayed_request.delay_ms = 0;

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
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

pub struct CommandExecutor {
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
    pub fn new(context: Arc<Context>, rx: mpsc::Receiver<CommandExecutionRequest>) -> Self {
        Self {
            command_resolver: CommandResolver::new(Arc::clone(&context)),
            scheduler: context.command_scheduler().clone(),
            rx: Arc::new(Mutex::new(rx)),
            semaphore: Arc::new(Semaphore::new(COMMAND_QUEUE_PARALLELISM)),
        }
    }

    /// Schedule initial commands to be executed.
    pub async fn schedule_commands(&self, requests: Vec<CommandExecutionRequest>) {
        for request in requests {
            self.scheduler.schedule(request).await;
        }
    }

    pub async fn run(&self) {
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
                            let permit = self.semaphore.clone().acquire_owned().await.unwrap();
                            pending_tasks.push(self.execute(request, permit));
                        }
                        None => {
                            tracing::error!("Command channel closed, shutting down executor");
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn execute(
        &self,
        request: CommandExecutionRequest,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        // Check if command has expired
        if request.is_expired() {
            tracing::warn!("Command {} expired, dropping", request.command.name());
            return;
        }

        let result = self.command_resolver.execute(&request.command).await;

        match result {
            CommandExecutionResult::Repeat { delay_ms } => {
                let new_request =
                    CommandExecutionRequest::new(request.command).with_delay(delay_ms);
                self.scheduler.schedule(new_request).await;
            }
            CommandExecutionResult::Completed => {
                tracing::trace!("Command {} completed", request.command.name());
            }
        }
    }
}
