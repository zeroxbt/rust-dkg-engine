use super::{
    command::{AbstractCommand, CommandResult, CommandStatus},
    dial_peers_command::DialPeersCommand,
    find_nodes_command::FindNodesCommand,
};
use crate::{
    commands::command::CommandName,
    constants::{
        COMMAND_QUEUE_PARALLELISM, DEFAULT_COMMAND_REPEAT_INTERVAL_IN_MILLS,
        MAX_COMMAND_DELAY_IN_MILLS, PERMANENT_COMMANDS,
    },
    context::Context,
};
use futures::stream::{FuturesUnordered, StreamExt};
use repository::models::command::Model;
use std::{cmp::min, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Mutex, Semaphore};

pub struct CommandExecutor {
    pub context: Arc<Context>,
    pub process_command_tx: mpsc::Sender<Box<dyn AbstractCommand>>,
    pub process_command_rx: Arc<Mutex<mpsc::Receiver<Box<dyn AbstractCommand>>>>,
    pub semaphore: Arc<Semaphore>,
}

impl CommandExecutor {
    pub async fn new(context: Arc<Context>) -> Self {
        let (tx, rx) = mpsc::channel::<Box<dyn AbstractCommand>>(COMMAND_QUEUE_PARALLELISM);

        Self {
            context,
            process_command_tx: tx,
            process_command_rx: Arc::new(Mutex::new(rx)),
            semaphore: Arc::new(Semaphore::new(COMMAND_QUEUE_PARALLELISM)),
        }
    }

    pub async fn execute_commands(&self) {
        let mut pending_tasks = FuturesUnordered::new();

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

    pub async fn schedule_commands(
        &self,
        mut schedule_command_rx: mpsc::Receiver<Box<dyn AbstractCommand>>,
    ) {
        loop {
            if let Some(command) = schedule_command_rx.recv().await {
                let delay = command.core().delay;

                self.add(command, delay, true).await.unwrap();
            }
        }
    }

    async fn add(
        &self,
        command: Box<dyn AbstractCommand>,
        delay: i64,
        insert: bool,
    ) -> Result<(), mpsc::error::SendError<Box<dyn AbstractCommand>>> {
        let delay = min(delay as u64, MAX_COMMAND_DELAY_IN_MILLS as u64);

        if insert {
            self.insert(command.as_ref()).await;
        }

        if delay > 0 {
            let process_command_tx = self.process_command_tx.clone();

            // Spawn a new async task
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let _ = process_command_tx.send(command).await;
            });

            Ok(()) // Return Ok immediately, without waiting for the delay
        } else {
            self.process_command_tx.send(command).await
        }
    }

    async fn execute(&self, command: Box<dyn AbstractCommand>) {
        let _ = self.semaphore.acquire().await.unwrap();
        let now = chrono::Utc::now().timestamp_millis();

        let command_core = command.core();

        if let Some(deadline_at) = command_core.deadline_at {
            if deadline_at <= now {
                tracing::warn!(
                    "Command ${:?} and ID ${} is too late...",
                    command_core.name,
                    command_core.id
                )
            }

            self.update(
                command.as_ref(),
                Some(CommandStatus::Expired.to_string()),
                None,
                None,
            )
            .await;

            return;
        }

        let delay = command_core.ready_at + command_core.delay - now;

        if delay > 0 {
            self.add(command, delay, false).await.unwrap();

            return;
        }

        self.update(
            command.as_ref(),
            Some(CommandStatus::Started.to_string()),
            Some(now),
            None,
        )
        .await;

        let result = command.execute(Arc::clone(&self.context)).await;

        match result {
            CommandResult::Retry => {
                let retries = command_core.retries;

                if retries < 1 {
                    self.handle_retry(command).await;
                    return;
                }

                command.retry_finished().await;
            }
            CommandResult::Repeat => {
                self.handle_repeat(command).await;
            }
            CommandResult::Completed => {
                self.handle_completed(command).await;
            }
        }
    }

    async fn handle_retry(&self, command: Box<dyn AbstractCommand>) {
        self.update(
            command.as_ref(),
            Some(CommandStatus::Repeating.to_string()),
            None,
            Some(command.core().retries - 1),
        )
        .await;

        let delay = command.core().period.unwrap_or_default();

        self.add(command, delay, false).await.unwrap();
    }

    async fn handle_repeat(&self, command: Box<dyn AbstractCommand>) {
        self.update(
            command.as_ref(),
            Some(CommandStatus::Repeating.to_string()),
            None,
            None,
        )
        .await;

        let period = command
            .core()
            .period
            .unwrap_or(DEFAULT_COMMAND_REPEAT_INTERVAL_IN_MILLS);

        self.add(command, period, false).await.unwrap();
    }

    async fn handle_completed(&self, command: Box<dyn AbstractCommand>) {
        self.update(
            command.as_ref(),
            Some(CommandStatus::Completed.to_string()),
            None,
            None,
        )
        .await;
    }

    async fn insert(&self, command: &dyn AbstractCommand) {
        self.context
            .repository_manager()
            .command_repository()
            .create_command(&command.to_command())
            .await
            .unwrap();
    }

    async fn update(
        &self,
        command: &dyn AbstractCommand,
        new_status: Option<String>,
        new_started_at: Option<i64>,
        new_retries: Option<i32>,
    ) {
        self.context
            .repository_manager()
            .command_repository()
            .update_command(
                &command.to_command(),
                new_status,
                new_started_at,
                new_retries,
            )
            .await
            .unwrap();
    }

    async fn delete(&self, name: &str) {
        self.context
            .repository_manager()
            .command_repository()
            .destroy_command(name)
            .await
            .unwrap()
    }

    async fn replay(&self) {
        tracing::info!("Replay pending/started commands from the database...");

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

        tracing::debug!("found: {} commands to be replayed.", pending_commands.len());

        for model in pending_commands {
            if let Some(command) = Self::model_to_command(model) {
                tracing::debug!("Adding command: {:?}", command.core().name);
                self.add(command, 0, false).await.unwrap();
            };
            // Adjust as per your needs. The JS version checks for parentId and other stuff.
        }
    }

    fn model_to_command(command_model: Model) -> Option<Box<dyn AbstractCommand>> {
        let command_name = command_model.name.parse::<CommandName>().unwrap();

        if PERMANENT_COMMANDS.contains(&command_name) {
            return None;
        }

        match command_name {
            CommandName::DialPeers => Some(Box::new(DialPeersCommand::from(command_model))),
            CommandName::FindNodes => Some(Box::new(FindNodesCommand::from(command_model))),
            CommandName::Default => None,
        }
    }
}
