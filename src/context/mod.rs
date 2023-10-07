use std::sync::Arc;

use blockchain::BlockchainManager;
use network::{action::NetworkAction, NetworkManager};
use repository::RepositoryManager;
use tokio::sync::mpsc::Sender;
use validation::ValidationManager;

use crate::{commands::command::Command, config::Config};

pub struct Context {
    config: Arc<Config>,
    network_action_tx: Sender<NetworkAction>,
    schedule_command_tx: Sender<Command>,
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager>,
    blockchain_manager: Arc<BlockchainManager>,
    validation_manager: Arc<ValidationManager>,
}

impl Context {
    pub fn new(
        config: Arc<Config>,
        network_action_tx: Sender<NetworkAction>,
        schedule_command_tx: Sender<Command>,
        repository_manager: Arc<RepositoryManager>,
        network_manager: Arc<NetworkManager>,
        blockchain_manager: Arc<BlockchainManager>,
        validation_manager: Arc<ValidationManager>,
    ) -> Self {
        Self {
            config,
            network_action_tx,
            schedule_command_tx,
            repository_manager,
            network_manager,
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

    pub fn network_manager(&self) -> &Arc<NetworkManager> {
        &self.network_manager
    }

    pub fn blockchain_manager(&self) -> &Arc<BlockchainManager> {
        &self.blockchain_manager
    }

    pub fn validation_manager(&self) -> &Arc<ValidationManager> {
        &self.validation_manager
    }

    pub fn network_action_tx(&self) -> &Sender<NetworkAction> {
        &self.network_action_tx
    }

    pub fn schedule_command_tx(&self) -> &Sender<Command> {
        &self.schedule_command_tx
    }
}
