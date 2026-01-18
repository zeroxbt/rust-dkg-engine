use std::sync::Arc;

use blockchain::BlockchainManager;
use network::NetworkManager;
use repository::RepositoryManager;
use tokio::sync::mpsc::Sender;
use validation::ValidationManager;

use crate::{
    commands::command_executor::CommandExecutionRequest,
    config::Config,
    network::{NetworkProtocols, SessionManager},
    services::{
        operation_manager::OperationManager, pending_storage_service::PendingStorageService,
        ual_service::UalService,
    },
    types::protocol::{FinalityResponseData, GetResponseData, StoreResponseData},
};

pub struct Context {
    config: Arc<Config>,
    schedule_command_tx: Sender<CommandExecutionRequest>,
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    blockchain_manager: Arc<BlockchainManager>,
    validation_manager: Arc<ValidationManager>,
    ual_service: Arc<UalService>,
    publish_operation_manager: Arc<OperationManager>,
    pending_storage_service: Arc<PendingStorageService>,
    store_session_manager: Arc<SessionManager<StoreResponseData>>,
    get_session_manager: Arc<SessionManager<GetResponseData>>,
    finality_session_manager: Arc<SessionManager<FinalityResponseData>>,
}

impl Context {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<Config>,
        schedule_command_tx: Sender<CommandExecutionRequest>,
        repository_manager: Arc<RepositoryManager>,
        network_manager: Arc<NetworkManager<NetworkProtocols>>,
        blockchain_manager: Arc<BlockchainManager>,
        validation_manager: Arc<ValidationManager>,
        ual_service: Arc<UalService>,
        publish_operation_manager: Arc<OperationManager>,
        pending_storage_service: Arc<PendingStorageService>,
        store_session_manager: Arc<SessionManager<StoreResponseData>>,
        get_session_manager: Arc<SessionManager<GetResponseData>>,
        finality_session_manager: Arc<SessionManager<FinalityResponseData>>,
    ) -> Self {
        Self {
            config,
            schedule_command_tx,
            repository_manager,
            network_manager,
            blockchain_manager,
            validation_manager,
            ual_service,
            publish_operation_manager,
            pending_storage_service,
            store_session_manager,
            get_session_manager,
            finality_session_manager,
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

    pub fn publish_operation_manager(&self) -> &Arc<OperationManager> {
        &self.publish_operation_manager
    }

    pub fn pending_storage_service(&self) -> &Arc<PendingStorageService> {
        &self.pending_storage_service
    }

    pub fn schedule_command_tx(&self) -> &Sender<CommandExecutionRequest> {
        &self.schedule_command_tx
    }

    pub fn store_session_manager(&self) -> &Arc<SessionManager<StoreResponseData>> {
        &self.store_session_manager
    }

    pub fn get_session_manager(&self) -> &Arc<SessionManager<GetResponseData>> {
        &self.get_session_manager
    }

    pub fn finality_session_manager(&self) -> &Arc<SessionManager<FinalityResponseData>> {
        &self.finality_session_manager
    }
}
