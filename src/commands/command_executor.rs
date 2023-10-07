use super::{
    command::{Command, CommandStatus},
    command_handler::CommandExecutionResult,
    command_resolver::CommandResolver,
    constants::{
        COMMAND_QUEUE_PARALLELISM, DEFAULT_COMMAND_DELAY_MS, DEFAULT_COMMAND_REPEAT_INTERVAL_MS,
        MAX_COMMAND_DELAY_MS, PERMANENT_COMMANDS,
    },
};
use crate::context::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use std::{cmp::min, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex, Semaphore};

pub struct CommandExecutor {
    pub context: Arc<Context>,
    pub process_command_tx: mpsc::Sender<Command>,
    pub process_command_rx: Arc<Mutex<mpsc::Receiver<Command>>>,
    pub semaphore: Arc<Semaphore>,
}

impl CommandExecutor {
    pub async fn new(context: Arc<Context>) -> Self {
        let (tx, rx) = mpsc::channel::<Command>(COMMAND_QUEUE_PARALLELISM);

        Self {
            context,
            process_command_tx: tx,
            process_command_rx: Arc::new(Mutex::new(rx)),
            semaphore: Arc::new(Semaphore::new(COMMAND_QUEUE_PARALLELISM)),
        }
    }

    pub async fn listen_and_execute_commands(&self) {
        let mut pending_tasks = FuturesUnordered::new();

        self.schedule_default_commands().await;
        self.replay().await;

        loop {
            let mut locked_rx = self.process_command_rx.lock().await;

            tokio::select! {
                _ = pending_tasks.select_next_some(), if !pending_tasks.is_empty() => {
                    // Continue the loop when a task completes.
                }
                command = locked_rx.recv(), if self.semaphore.available_permits() > 0 => {
                    if let Some(command) = command {
                        pending_tasks.push(self.execute(command));
                    }
                }
            }
        }
    }

    async fn schedule_default_commands(&self) {
        for command_name in PERMANENT_COMMANDS {
            let Some(command) = command_name.to_command() else {
                tracing::warn!(
                    "Permanent command: {} has no default implementation!",
                    command_name
                );
                continue;
            };
            self.add(command, DEFAULT_COMMAND_DELAY_MS, true)
                .await
                .unwrap();
        }
    }

    pub async fn listen_and_schedule_commands(
        &self,
        mut schedule_command_rx: mpsc::Receiver<Command>,
    ) {
        loop {
            if let Some(command) = schedule_command_rx.recv().await {
                let delay = command.delay;

                self.add(command, delay, true).await.unwrap();
            }
        }
    }

    async fn add(
        &self,
        command: Command,
        delay: i64,
        insert: bool,
    ) -> Result<(), mpsc::error::SendError<Command>> {
        let delay = min(delay as u64, MAX_COMMAND_DELAY_MS as u64);

        if insert {
            self.insert(&command).await;
        }

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

    async fn execute(&self, command: Command) {
        let _ = self.semaphore.acquire().await.unwrap();

        let now = chrono::Utc::now().timestamp_millis();

        if let Some(deadline_at) = command.deadline_at {
            if deadline_at <= now {
                tracing::warn!(
                    "Command ${:?} and ID ${} is too late...",
                    command.name,
                    command.id
                )
            }

            self.update(
                &command,
                Some(CommandStatus::Expired.to_string()),
                None,
                None,
            )
            .await;

            return;
        }

        let delay = command.ready_at + command.delay - now;

        if delay > 0 {
            self.add(command, delay, false).await.unwrap();

            return;
        }

        self.update(
            &command,
            Some(CommandStatus::Started.to_string()),
            Some(now),
            None,
        )
        .await;

        let command_handler = CommandResolver::resolve(&command.name, Arc::clone(&self.context));

        let result = command_handler.execute(&command).await;

        match result {
            CommandExecutionResult::Retry => {
                let retries = command.retries;

                if retries < 1 {
                    self.handle_retry(&command).await;
                    return;
                }

                command_handler.retry_finished().await;
            }
            CommandExecutionResult::Repeat => {
                self.handle_repeat(&command).await;
            }
            CommandExecutionResult::Completed => {
                self.handle_completed(&command).await;
            }
        }
    }

    async fn handle_retry(&self, command: &Command) {
        self.update(
            command,
            Some(CommandStatus::Repeating.to_string()),
            None,
            Some(command.retries - 1),
        )
        .await;

        let delay = command.period.unwrap_or_default();

        self.add(command.clone(), delay, false).await.unwrap();
    }

    async fn handle_repeat(&self, command: &Command) {
        self.update(
            command,
            Some(CommandStatus::Repeating.to_string()),
            None,
            None,
        )
        .await;

        let period = command.period.unwrap_or(DEFAULT_COMMAND_REPEAT_INTERVAL_MS);

        self.add(command.clone(), period, false).await.unwrap();
    }

    async fn handle_completed(&self, command: &Command) {
        self.update(
            command,
            Some(CommandStatus::Completed.to_string()),
            None,
            None,
        )
        .await;
    }

    async fn insert(&self, command: &Command) {
        self.context
            .repository_manager()
            .command_repository()
            .create_command(&command.to_model())
            .await
            .unwrap();
    }

    async fn update(
        &self,
        command: &Command,
        new_status: Option<String>,
        new_started_at: Option<i64>,
        new_retries: Option<i32>,
    ) {
        self.context
            .repository_manager()
            .command_repository()
            .update_command(&command.to_model(), new_status, new_started_at, new_retries)
            .await
            .unwrap();
    }

    async fn replay(&self) {
        tracing::info!("Replaying pending/started commands from the repository...");

        let pending_commands = self
            .context
            .repository_manager()
            .command_repository()
            .get_commands_with_status(vec![
                CommandStatus::Pending.to_string(),
                CommandStatus::Started.to_string(),
                CommandStatus::Repeating.to_string(),
            ])
            .await
            .unwrap();

        for model in pending_commands {
            self.add(Command::from(model), 0, false).await.unwrap();
        }
    }
}
