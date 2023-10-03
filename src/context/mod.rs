use std::sync::Arc;

use blockchain::BlockchainManager;
use network::command::NetworkCommand;
use repository::RepositoryManager;
use tokio::sync::mpsc::Sender;
use validation::ValidationManager;

use crate::{commands::command::AbstractCommand, config::Config};

pub struct Context {
    config: Arc<Config>,
    network_command_tx: Sender<NetworkCommand>,
    schedule_command_tx: Sender<Box<dyn AbstractCommand>>,
    repository_manager: Arc<RepositoryManager>,
    blockchain_manager: Arc<BlockchainManager>,
    validation_manager: Arc<ValidationManager>,
}

impl Context {
    pub fn new(
        config: Arc<Config>,
        network_command_tx: Sender<NetworkCommand>,
        schedule_command_tx: Sender<Box<dyn AbstractCommand>>,
        repository_manager: Arc<RepositoryManager>,
        blockchain_manager: Arc<BlockchainManager>,
        validation_manager: Arc<ValidationManager>,
    ) -> Self {
        Self {
            config,
            network_command_tx,
            schedule_command_tx,
            repository_manager,
            blockchain_manager,
            validation_manager,
        }
    }

    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    pub fn repository_manager(&self) -> &Arc<RepositoryManager> {
        &self.repository_manager
    }

    pub fn blockchain_manager(&self) -> &Arc<BlockchainManager> {
        &self.blockchain_manager
    }

    pub fn validation_manager(&self) -> &Arc<ValidationManager> {
        &self.validation_manager
    }

    pub fn network_command_tx(&self) -> &Sender<NetworkCommand> {
        &self.network_command_tx
    }

    pub fn schedule_command_tx(&self) -> &Sender<Box<dyn AbstractCommand>> {
        &self.schedule_command_tx
    }
}
