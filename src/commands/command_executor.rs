use std::{cmp::min, sync::Arc, time::Duration};

use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{Mutex, Semaphore, mpsc};

use super::{
    command_registry::Command,
    constants::{COMMAND_QUEUE_PARALLELISM, MAX_COMMAND_DELAY_MS, MAX_COMMAND_LIFETIME_MS},
};
use crate::{commands::command_registry::CommandResolver, context::Context};

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

pub struct CommandExecutor {
    pub command_resolver: CommandResolver,
    pub process_command_tx: mpsc::Sender<CommandExecutionRequest>,
    pub process_command_rx: Arc<Mutex<mpsc::Receiver<CommandExecutionRequest>>>,
    pub semaphore: Arc<Semaphore>,
}

// TODO:
//   - add priority based scheduling
//   - switch to redis based worker queues or something similar
//   - add error handling

impl CommandExecutor {
    pub fn new(context: Arc<Context>) -> Self {
        let (tx, rx) = mpsc::channel::<CommandExecutionRequest>(COMMAND_QUEUE_PARALLELISM);

        Self {
            command_resolver: CommandResolver::new(context),
            process_command_tx: tx,
            process_command_rx: Arc::new(Mutex::new(rx)),
            semaphore: Arc::new(Semaphore::new(COMMAND_QUEUE_PARALLELISM)),
        }
    }

    /// Schedule initial commands to be executed
    pub async fn schedule_commands(&self, requests: Vec<CommandExecutionRequest>) {
        for request in requests {
            self.enqueue(request).await.unwrap();
        }
    }

    pub async fn listen_and_execute_commands(&self) {
        let mut pending_tasks = FuturesUnordered::new();

        loop {
            let mut locked_rx = self.process_command_rx.lock().await;

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

    pub async fn listen_and_schedule_commands(
        &self,
        mut schedule_command_rx: mpsc::Receiver<CommandExecutionRequest>,
    ) {
        loop {
            if let Some(request) = schedule_command_rx.recv().await {
                self.enqueue(request).await.unwrap();
            }
        }
    }

    async fn enqueue(
        &self,
        request: CommandExecutionRequest,
    ) -> Result<(), mpsc::error::SendError<CommandExecutionRequest>> {
        let delay = min(request.delay_ms.max(0) as u64, MAX_COMMAND_DELAY_MS as u64);

        if delay > 0 {
            let process_command_tx = self.process_command_tx.clone();
            let mut delayed_request = request;
            delayed_request.delay_ms = 0;

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let _ = process_command_tx.send(delayed_request).await;
            });

            Ok(())
        } else {
            self.process_command_tx.send(request).await
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
                self.enqueue(new_request).await.unwrap();
            }
            CommandExecutionResult::Completed => {
                tracing::trace!("Command {} completed", request.command.name());
            }
        }
    }
}
