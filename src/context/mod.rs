use std::sync::Arc;

use blockchain::BlockchainManager;
use network::NetworkManager;
use repository::RepositoryManager;
use tokio::sync::mpsc::Sender;
use triple_store::TripleStoreManager;
use validation::ValidationManager;

use crate::{
    commands::command_executor::CommandExecutionRequest,
    config::Config,
    controllers::rpc_controller::{
        NetworkProtocols,
        messages::{FinalityResponseData, GetResponseData, StoreResponseData},
    },
    services::{
        OperationService, RequestTracker, ResponseChannels,
        pending_storage_service::PendingStorageService,
    },
};

pub struct Context {
    config: Arc<Config>,
    schedule_command_tx: Sender<CommandExecutionRequest>,
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    blockchain_manager: Arc<BlockchainManager>,
    validation_manager: Arc<ValidationManager>,
    triple_store_manager: Arc<TripleStoreManager>,
    publish_operation_manager: Arc<OperationService>,
    pending_storage_service: Arc<PendingStorageService>,
    request_tracker: Arc<RequestTracker>,
    store_response_channels: Arc<ResponseChannels<StoreResponseData>>,
    get_response_channels: Arc<ResponseChannels<GetResponseData>>,
    finality_response_channels: Arc<ResponseChannels<FinalityResponseData>>,
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
        triple_store_manager: Arc<TripleStoreManager>,
        publish_operation_manager: Arc<OperationService>,
        pending_storage_service: Arc<PendingStorageService>,
        request_tracker: Arc<RequestTracker>,
        store_response_channels: Arc<ResponseChannels<StoreResponseData>>,
        get_response_channels: Arc<ResponseChannels<GetResponseData>>,
        finality_response_channels: Arc<ResponseChannels<FinalityResponseData>>,
    ) -> Self {
        Self {
            config,
            schedule_command_tx,
            repository_manager,
            network_manager,
            blockchain_manager,
            validation_manager,
            triple_store_manager,
            publish_operation_manager,
            pending_storage_service,
            request_tracker,
            store_response_channels,
            get_response_channels,
            finality_response_channels,
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

    pub fn triple_store_manager(&self) -> &Arc<TripleStoreManager> {
        &self.triple_store_manager
    }

    pub fn publish_operation_manager(&self) -> &Arc<OperationService> {
        &self.publish_operation_manager
    }

    pub fn pending_storage_service(&self) -> &Arc<PendingStorageService> {
        &self.pending_storage_service
    }

    pub fn schedule_command_tx(&self) -> &Sender<CommandExecutionRequest> {
        &self.schedule_command_tx
    }

    pub fn request_tracker(&self) -> &Arc<RequestTracker> {
        &self.request_tracker
    }

    pub fn store_response_channels(&self) -> &Arc<ResponseChannels<StoreResponseData>> {
        &self.store_response_channels
    }

    pub fn get_response_channels(&self) -> &Arc<ResponseChannels<GetResponseData>> {
        &self.get_response_channels
    }

    pub fn finality_response_channels(&self) -> &Arc<ResponseChannels<FinalityResponseData>> {
        &self.finality_response_channels
    }
}
