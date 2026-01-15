use std::{cmp::min, sync::Arc, time::Duration};

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{Mutex, Semaphore, mpsc};

use super::{
    command::Command,
    command_resolver::CommandResolver,
    constants::{
        COMMAND_QUEUE_PARALLELISM, DEFAULT_COMMAND_REPEAT_INTERVAL_MS, MAX_COMMAND_DELAY_MS,
    },
};
use crate::{context::Context, types::traits::command::CommandExecutionResult};

pub struct CommandExecutor {
    // pub repository_manager: Arc<RepositoryManager>,
    pub command_resolver: CommandResolver,
    pub process_command_tx: mpsc::Sender<Command>,
    pub process_command_rx: Arc<Mutex<mpsc::Receiver<Command>>>,
    pub semaphore: Arc<Semaphore>,
}

// TODO:
//   - add priority based scheduling
//   - switch to redis based worker queues or something similar
//   - add error handling

impl CommandExecutor {
    pub async fn new(context: Arc<Context>) -> Self {
        let (tx, rx) = mpsc::channel::<Command>(COMMAND_QUEUE_PARALLELISM);

        Self {
            // repository_manager: Arc::clone(context.repository_manager()),
            command_resolver: CommandResolver::new(context),
            process_command_tx: tx,
            process_command_rx: Arc::new(Mutex::new(rx)),
            semaphore: Arc::new(Semaphore::new(COMMAND_QUEUE_PARALLELISM)),
        }
    }

    pub async fn listen_and_execute_commands(&self) {
        let mut pending_tasks = FuturesUnordered::new();

        self.schedule_default_commands().await;
        // NOTE: Commented out - commands are no longer persisted/replayed from DB
        // self.replay().await;

        loop {
            let mut locked_rx = self.process_command_rx.lock().await;

            tokio::select! {
                _ = pending_tasks.select_next_some(), if !pending_tasks.is_empty() => {
                    // Continue the loop when a task completes.
                }
                command = locked_rx.recv() => {
                    match command {
                        Some(command) => {
                            drop(locked_rx);
                            let permit = self.semaphore.clone().acquire_owned().await.unwrap();
                            pending_tasks.push(self.execute(command, permit));
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

    async fn schedule_default_commands(&self) {
        for command in self.command_resolver.periodic_commands() {
            self.add(command, 0).await.unwrap();
        }
    }

    pub async fn listen_and_schedule_commands(
        &self,
        mut schedule_command_rx: mpsc::Receiver<Command>,
    ) {
        loop {
            if let Some(command) = schedule_command_rx.recv().await {
                let delay = command.delay;

                self.add(command, delay).await.unwrap();
            }
        }
    }

    async fn add(
        &self,
        command: Command,
        delay: i64,
    ) -> Result<(), mpsc::error::SendError<Command>> {
        let delay = min(delay as u64, MAX_COMMAND_DELAY_MS as u64);

        // NOTE: Commented out - commands are no longer persisted to DB
        // self.insert(&command).await;

        if delay > 0 {
            let process_command_tx = self.process_command_tx.clone();

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let _ = process_command_tx.send(command).await;
            });

            Ok(())
        } else {
            self.process_command_tx.send(command).await
        }
    }

    async fn execute(&self, command: Command, _permit: tokio::sync::OwnedSemaphorePermit) {
        let now = chrono::Utc::now().timestamp_millis();

        if let Some(deadline_at) = command.deadline_at {
            if deadline_at <= now {
                tracing::warn!(
                    "Command ${:?} and ID ${} is too late...",
                    command.name,
                    command.id
                )
            }

            // NOTE: Commented out - commands are no longer persisted to DB
            // self.update(
            //     &command,
            //     Some(CommandStatus::Expired.to_string()),
            //     None,
            //     None,
            // )
            // .await;

            return;
        }

        let delay = command.ready_at + command.delay - now;

        if delay > 0 {
            self.add(command, delay).await.unwrap();

            return;
        }

        // NOTE: Commented out - commands are no longer persisted to DB
        // self.update(
        //     &command,
        //     Some(CommandStatus::Started.to_string()),
        //     Some(now),
        //     None,
        // )
        // .await;

        let Some(command_handler) = self.command_resolver.resolve(&command.name.to_string()) else {
            tracing::error!("Unknown command: {}", command.name);
            return;
        };

        let result = command_handler.execute(&command).await;

        match result {
            CommandExecutionResult::Retry => {
                if command.retries > 0 {
                    let mut retry_command = command.clone();
                    retry_command.retries -= 1;
                    let delay = retry_command.period.unwrap_or_default();
                    self.add(retry_command, delay).await.unwrap();
                } else {
                    command_handler.retry_finished().await;
                }
            }
            CommandExecutionResult::Repeat => {
                let repeat_command = command.clone();
                let period = repeat_command
                    .period
                    .unwrap_or(DEFAULT_COMMAND_REPEAT_INTERVAL_MS);
                self.add(repeat_command, period).await.unwrap();
            }
            CommandExecutionResult::Completed => {
                tracing::trace!("Command {} completed", command.name);
            }
        }
    }

    // NOTE: Commented out - commands are no longer persisted to DB
    // async fn handle_retry(&self, command: &Command) {
    //     self.update(
    //         command,
    //         Some(CommandStatus::Repeating.to_string()),
    //         None,
    //         Some(command.retries - 1),
    //     )
    //     .await;
    //
    //     let delay = command.period.unwrap_or_default();
    //
    //     self.add(command.clone(), delay).await.unwrap();
    // }

    // NOTE: Commented out - commands are no longer persisted to DB
    // async fn handle_repeat(&self, command: &Command) {
    //     self.update(
    //         command,
    //         Some(CommandStatus::Repeating.to_string()),
    //         None,
    //         None,
    //     )
    //     .await;
    //
    //     let period = command.period.unwrap_or(DEFAULT_COMMAND_REPEAT_INTERVAL_MS);
    //
    //     self.add(command.clone(), period).await.unwrap();
    // }

    // NOTE: Commented out - commands are no longer persisted to DB
    // async fn handle_completed(&self, command: &Command) {
    //     self.update(
    //         command,
    //         Some(CommandStatus::Completed.to_string()),
    //         None,
    //         None,
    //     )
    //     .await;
    // }

    // NOTE: Commented out - commands are no longer persisted to DB
    // async fn insert(&self, command: &Command) {
    //     self.repository_manager
    //         .command_repository()
    //         .create_command(&command.to_model())
    //         .await
    //         .unwrap();
    // }

    // NOTE: Commented out - commands are no longer persisted to DB
    // async fn update(
    //     &self,
    //     command: &Command,
    //     new_status: Option<String>,
    //     new_started_at: Option<i64>,
    //     new_retries: Option<i32>,
    // ) {
    //     self.repository_manager
    //         .command_repository()
    //         .update_command(&command.to_model(), new_status, new_started_at, new_retries)
    //         .await
    //         .unwrap();
    // }

    // NOTE: Commented out - commands are no longer persisted/replayed from DB
    // async fn replay(&self) {
    //     tracing::info!("Replaying pending/started commands from the repository...");
    //
    //     let pending_commands = self
    //         .repository_manager
    //         .command_repository()
    //         .get_commands_with_status(vec![
    //             CommandStatus::Pending.to_string(),
    //             CommandStatus::Started.to_string(),
    //             CommandStatus::Repeating.to_string(),
    //         ])
    //         .await
    //         .unwrap();
    //
    //     for model in pending_commands {
    //         self.add(Command::from(model), 0).await.unwrap();
    //     }
    // }
}
