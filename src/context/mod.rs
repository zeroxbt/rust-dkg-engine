use std::sync::Arc;

use crate::{
    commands::command_executor::CommandScheduler,
    config::Config,
    managers::{
        blockchain::BlockchainManager,
        network::{
            NetworkManager,
            messages::{
                BatchGetResponseData, FinalityResponseData, GetResponseData, StoreResponseData,
            },
        },
        repository::RepositoryManager,
    },
    operations::{BatchGetOperation, GetOperation, PublishOperation},
    services::{
        GetValidationService, PeerDiscoveryTracker, ResponseChannels, TripleStoreService,
        operation::OperationService as GenericOperationService,
        pending_storage_service::PendingStorageService,
    },
};

pub(crate) struct Context {
    config: Arc<Config>,
    command_scheduler: CommandScheduler,
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager>,
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_service: Arc<TripleStoreService>,
    get_validation_service: Arc<GetValidationService>,
    pending_storage_service: Arc<PendingStorageService>,
    peer_discovery_tracker: Arc<PeerDiscoveryTracker>,
    store_response_channels: Arc<ResponseChannels<StoreResponseData>>,
    get_response_channels: Arc<ResponseChannels<GetResponseData>>,
    finality_response_channels: Arc<ResponseChannels<FinalityResponseData>>,
    batch_get_response_channels: Arc<ResponseChannels<BatchGetResponseData>>,
    get_operation_service: Arc<GenericOperationService<GetOperation>>,
    publish_operation_service: Arc<GenericOperationService<PublishOperation>>,
    batch_get_operation_service: Arc<GenericOperationService<BatchGetOperation>>,
}

impl Context {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        config: Arc<Config>,
        command_scheduler: CommandScheduler,
        repository_manager: Arc<RepositoryManager>,
        network_manager: Arc<NetworkManager>,
        blockchain_manager: Arc<BlockchainManager>,
        triple_store_service: Arc<TripleStoreService>,
        get_validation_service: Arc<GetValidationService>,
        pending_storage_service: Arc<PendingStorageService>,
        peer_discovery_tracker: Arc<PeerDiscoveryTracker>,
        store_response_channels: Arc<ResponseChannels<StoreResponseData>>,
        get_response_channels: Arc<ResponseChannels<GetResponseData>>,
        finality_response_channels: Arc<ResponseChannels<FinalityResponseData>>,
        batch_get_response_channels: Arc<ResponseChannels<BatchGetResponseData>>,
        get_operation_service: Arc<GenericOperationService<GetOperation>>,
        publish_operation_service: Arc<GenericOperationService<PublishOperation>>,
        batch_get_operation_service: Arc<GenericOperationService<BatchGetOperation>>,
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
            peer_discovery_tracker,
            store_response_channels,
            get_response_channels,
            finality_response_channels,
            batch_get_response_channels,
            get_operation_service,
            publish_operation_service,
            batch_get_operation_service,
        }
    }

    pub(crate) fn config(&self) -> &Arc<Config> {
        &self.config
    }

    pub(crate) fn command_scheduler(&self) -> &CommandScheduler {
        &self.command_scheduler
    }

    pub(crate) fn repository_manager(&self) -> &Arc<RepositoryManager> {
        &self.repository_manager
    }

    pub(crate) fn network_manager(&self) -> &Arc<NetworkManager> {
        &self.network_manager
    }

    pub(crate) fn blockchain_manager(&self) -> &Arc<BlockchainManager> {
        &self.blockchain_manager
    }

    pub(crate) fn triple_store_service(&self) -> &Arc<TripleStoreService> {
        &self.triple_store_service
    }

    pub(crate) fn get_validation_service(&self) -> &Arc<GetValidationService> {
        &self.get_validation_service
    }

    pub(crate) fn pending_storage_service(&self) -> &Arc<PendingStorageService> {
        &self.pending_storage_service
    }

    pub(crate) fn peer_discovery_tracker(&self) -> &Arc<PeerDiscoveryTracker> {
        &self.peer_discovery_tracker
    }

    pub(crate) fn store_response_channels(&self) -> &Arc<ResponseChannels<StoreResponseData>> {
        &self.store_response_channels
    }

    pub(crate) fn get_response_channels(&self) -> &Arc<ResponseChannels<GetResponseData>> {
        &self.get_response_channels
    }

    pub(crate) fn finality_response_channels(
        &self,
    ) -> &Arc<ResponseChannels<FinalityResponseData>> {
        &self.finality_response_channels
    }

    pub(crate) fn get_operation_service(&self) -> &Arc<GenericOperationService<GetOperation>> {
        &self.get_operation_service
    }

    pub(crate) fn publish_operation_service(
        &self,
    ) -> &Arc<GenericOperationService<PublishOperation>> {
        &self.publish_operation_service
    }

    pub(crate) fn batch_get_response_channels(
        &self,
    ) -> &Arc<ResponseChannels<BatchGetResponseData>> {
        &self.batch_get_response_channels
    }

    pub(crate) fn batch_get_operation_service(
        &self,
    ) -> &Arc<GenericOperationService<BatchGetOperation>> {
        &self.batch_get_operation_service
    }
}
