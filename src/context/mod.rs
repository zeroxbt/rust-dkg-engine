use std::sync::Arc;

use blockchain::BlockchainManager;
use network::command::NetworkCommand;
use repository::RepositoryManager;
use tokio::sync::mpsc::Sender;

use crate::commands::command::AbstractCommand;

pub struct Context {
    network_command_tx: Sender<NetworkCommand>,
    schedule_command_tx: Sender<Box<dyn AbstractCommand>>,
    repository_manager: Arc<RepositoryManager>,
    blockchain_manager: Arc<BlockchainManager>,
}

impl Context {
    pub fn new(
        network_command_tx: Sender<NetworkCommand>,
        schedule_command_tx: Sender<Box<dyn AbstractCommand>>,
        repository_manager: Arc<RepositoryManager>,
        blockchain_manager: Arc<BlockchainManager>,
    ) -> Self {
        Self {
            network_command_tx,
            schedule_command_tx,
            repository_manager,
            blockchain_manager,
        }
    }

    pub fn repository_manager(&self) -> &Arc<RepositoryManager> {
        &self.repository_manager
    }

    pub fn blockchain_manager(&self) -> &Arc<BlockchainManager> {
        &self.blockchain_manager
    }

    pub fn network_command_tx(&self) -> &Sender<NetworkCommand> {
        &self.network_command_tx
    }

    pub fn schedule_command_tx(&self) -> &Sender<Box<dyn AbstractCommand>> {
        &self.schedule_command_tx
    }
}
