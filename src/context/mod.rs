use std::sync::Arc;

use blockchain::BlockchainManager;
use network::NetworkManager;
use repository::RepositoryManager;
use tokio::sync::mpsc::Sender;
use validation::ValidationManager;

use crate::{
    commands::command::Command,
    config::Config,
    network::{NetworkProtocols, SessionManager},
    services::{
        pending_storage_service::PendingStorageService, publish_service::PublishService,
        sharding_table_service::ShardingTableService, ual_service::UalService,
    },
    types::protocol::{GetResponseData, StoreResponseData},
};

pub struct Context {
    config: Arc<Config>,
    schedule_command_tx: Sender<Command>,
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    blockchain_manager: Arc<BlockchainManager>,
    validation_manager: Arc<ValidationManager>,
    ual_service: Arc<UalService>,
    sharding_table_service: Arc<ShardingTableService>,
    publish_service: Arc<PublishService>,
    pending_storage_service: Arc<PendingStorageService>,
    store_session_manager: Arc<SessionManager<StoreResponseData>>,
    get_session_manager: Arc<SessionManager<GetResponseData>>,
}

impl Context {
    pub fn new(
        config: Arc<Config>,
        schedule_command_tx: Sender<Command>,
        repository_manager: Arc<RepositoryManager>,
        network_manager: Arc<NetworkManager<NetworkProtocols>>,
        blockchain_manager: Arc<BlockchainManager>,
        validation_manager: Arc<ValidationManager>,
        ual_service: Arc<UalService>,
        sharding_table_service: Arc<ShardingTableService>,
        publish_service: Arc<PublishService>,
        pending_storage_service: Arc<PendingStorageService>,
        store_session_manager: Arc<SessionManager<StoreResponseData>>,
        get_session_manager: Arc<SessionManager<GetResponseData>>,
    ) -> Self {
        Self {
            config,
            schedule_command_tx,
            repository_manager,
            network_manager,
            blockchain_manager,
            validation_manager,
            ual_service,
            sharding_table_service,
            publish_service,
            pending_storage_service,
            store_session_manager,
            get_session_manager,
        }
    }

    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    pub fn repository_manager(&self) -> &Arc<RepositoryManager> {
        &self.repository_manager
    }

    pub fn network_manager(&self) -> &Arc<NetworkManager<NetworkProtocols>> {
        &self.network_manager
    }

    pub fn blockchain_manager(&self) -> &Arc<BlockchainManager> {
        &self.blockchain_manager
    }

    pub fn validation_manager(&self) -> &Arc<ValidationManager> {
        &self.validation_manager
    }

    pub fn ual_service(&self) -> &Arc<UalService> {
        &self.ual_service
    }

    pub fn sharding_table_service(&self) -> &Arc<ShardingTableService> {
        &self.sharding_table_service
    }

    pub fn publish_service(&self) -> &Arc<PublishService> {
        &self.publish_service
    }

    pub fn pending_storage_service(&self) -> &Arc<PendingStorageService> {
        &self.pending_storage_service
    }

    pub fn schedule_command_tx(&self) -> &Sender<Command> {
        &self.schedule_command_tx
    }

    pub fn store_session_manager(&self) -> &Arc<SessionManager<StoreResponseData>> {
        &self.store_session_manager
    }

    pub fn get_session_manager(&self) -> &Arc<SessionManager<GetResponseData>> {
        &self.get_session_manager
    }
}
