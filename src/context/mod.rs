use network::command::NetworkCommand;
use repository::RepositoryManager;
use tokio::sync::mpsc::Sender;

use crate::commands::command::AbstractCommand;

pub struct Context {
    network_command_tx: Sender<NetworkCommand>,
    schedule_command_tx: Sender<Box<dyn AbstractCommand>>,
    repository_manager: RepositoryManager,
}

impl Context {
    pub fn new(
        network_command_tx: Sender<NetworkCommand>,
        schedule_command_tx: Sender<Box<dyn AbstractCommand>>,
        repository_manager: RepositoryManager,
    ) -> Self {
        Self {
            network_command_tx,
            schedule_command_tx,
            repository_manager,
        }
    }

    pub fn get_repository_manager(&self) -> &RepositoryManager {
        &self.repository_manager
    }

    pub fn get_network_command_tx(&self) -> &Sender<NetworkCommand> {
        &self.network_command_tx
    }

    pub fn get_schedule_command_tx(&self) -> &Sender<Box<dyn AbstractCommand>> {
        &self.schedule_command_tx
    }
}
