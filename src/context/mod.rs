use std::sync::Arc;

use blockchain::BlockchainManager;
use network::NetworkManager;
use repository::RepositoryManager;

use crate::{
    commands::command_executor::CommandScheduler,
    config::Config,
    controllers::rpc_controller::{
        NetworkProtocols,
        messages::{FinalityResponseData, GetResponseData, StoreResponseData},
    },
    operations::{GetOperation, PublishOperation},
    services::{
        GetValidationService, ResponseChannels, TripleStoreService,
        operation::OperationService as GenericOperationService,
        pending_storage_service::PendingStorageService,
    },
};

pub struct Context {
    config: Arc<Config>,
    command_scheduler: CommandScheduler,
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_service: Arc<TripleStoreService>,
    get_validation_service: Arc<GetValidationService>,
    pending_storage_service: Arc<PendingStorageService>,
    store_response_channels: Arc<ResponseChannels<StoreResponseData>>,
    get_response_channels: Arc<ResponseChannels<GetResponseData>>,
    finality_response_channels: Arc<ResponseChannels<FinalityResponseData>>,
    get_operation_service: Arc<GenericOperationService<GetOperation>>,
    publish_operation_service: Arc<GenericOperationService<PublishOperation>>,
}

impl Context {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<Config>,
        command_scheduler: CommandScheduler,
        repository_manager: Arc<RepositoryManager>,
        network_manager: Arc<NetworkManager<NetworkProtocols>>,
        blockchain_manager: Arc<BlockchainManager>,
        triple_store_service: Arc<TripleStoreService>,
        get_validation_service: Arc<GetValidationService>,
        pending_storage_service: Arc<PendingStorageService>,
        store_response_channels: Arc<ResponseChannels<StoreResponseData>>,
        get_response_channels: Arc<ResponseChannels<GetResponseData>>,
        finality_response_channels: Arc<ResponseChannels<FinalityResponseData>>,
        get_operation_service: Arc<GenericOperationService<GetOperation>>,
        publish_operation_service: Arc<GenericOperationService<PublishOperation>>,
    ) -> Self {
        Self {
            config,
            command_scheduler,
            repository_manager,
            network_manager,
            blockchain_manager,
            triple_store_service,
            get_validation_service,
            pending_storage_service,
            store_response_channels,
            get_response_channels,
            finality_response_channels,
            get_operation_service,
            publish_operation_service,
        }
    }

    pub fn config(&self) -> &Arc<Config> {
        &self.config
    }

    pub fn command_scheduler(&self) -> &CommandScheduler {
        &self.command_scheduler
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

    pub fn triple_store_service(&self) -> &Arc<TripleStoreService> {
        &self.triple_store_service
    }

    pub fn get_validation_service(&self) -> &Arc<GetValidationService> {
        &self.get_validation_service
    }

    pub fn pending_storage_service(&self) -> &Arc<PendingStorageService> {
        &self.pending_storage_service
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

    pub fn get_operation_service(&self) -> &Arc<GenericOperationService<GetOperation>> {
        &self.get_operation_service
    }

    pub fn publish_operation_service(&self) -> &Arc<GenericOperationService<PublishOperation>> {
        &self.publish_operation_service
    }
}
