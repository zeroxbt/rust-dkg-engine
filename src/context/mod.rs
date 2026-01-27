use std::sync::Arc;

use crate::{
    commands::command_executor::CommandScheduler,
    config::Config,
    managers::{
        Managers,
        network::messages::{
            BatchGetResponseData, FinalityResponseData, GetResponseData, StoreResponseData,
        },
    },
    services::{
        GetValidationService, PeerDiscoveryTracker, ResponseChannels, Services, TripleStoreService,
    },
};

pub(crate) struct Context {
    config: Arc<Config>,
    command_scheduler: CommandScheduler,
    managers: Managers,
    services: Services,
}

impl Context {
    pub(crate) fn new(
        config: Arc<Config>,
        command_scheduler: CommandScheduler,
        managers: Managers,
        services: Services,
    ) -> Self {
        Self {
            config,
            command_scheduler,
            managers,
            services,
        }
    }

    pub(crate) fn config(&self) -> &Arc<Config> {
        &self.config
    }

    pub(crate) fn command_scheduler(&self) -> &CommandScheduler {
        &self.command_scheduler
    }

    // Manager accessors
    pub(crate) fn repository_manager(&self) -> &Arc<crate::managers::RepositoryManager> {
        &self.managers.repository
    }

    pub(crate) fn network_manager(&self) -> &Arc<crate::managers::NetworkManager> {
        &self.managers.network
    }

    pub(crate) fn blockchain_manager(&self) -> &Arc<crate::managers::BlockchainManager> {
        &self.managers.blockchain
    }

    // Service accessors
    pub(crate) fn triple_store_service(&self) -> &Arc<TripleStoreService> {
        &self.services.triple_store
    }

    pub(crate) fn get_validation_service(&self) -> &Arc<GetValidationService> {
        &self.services.get_validation
    }

    pub(crate) fn pending_storage_service(&self) -> &Arc<crate::services::PendingStorageService> {
        &self.services.pending_storage
    }

    pub(crate) fn peer_discovery_tracker(&self) -> &Arc<PeerDiscoveryTracker> {
        &self.services.peer_discovery_tracker
    }

    // Response channel accessors
    pub(crate) fn store_response_channels(&self) -> &Arc<ResponseChannels<StoreResponseData>> {
        &self.services.response_channels.store
    }

    pub(crate) fn get_response_channels(&self) -> &Arc<ResponseChannels<GetResponseData>> {
        &self.services.response_channels.get
    }

    pub(crate) fn finality_response_channels(
        &self,
    ) -> &Arc<ResponseChannels<FinalityResponseData>> {
        &self.services.response_channels.finality
    }

    pub(crate) fn batch_get_response_channels(
        &self,
    ) -> &Arc<ResponseChannels<BatchGetResponseData>> {
        &self.services.response_channels.batch_get
    }

    // Operation service accessors
    pub(crate) fn get_operation_service(
        &self,
    ) -> &Arc<crate::services::OperationService<crate::operations::GetOperation>> {
        &self.services.get_operation
    }

    pub(crate) fn publish_operation_service(
        &self,
    ) -> &Arc<crate::services::OperationService<crate::operations::PublishOperation>> {
        &self.services.publish_operation
    }

    pub(crate) fn batch_get_operation_service(
        &self,
    ) -> &Arc<crate::services::OperationService<crate::operations::BatchGetOperation>> {
        &self.services.batch_get_operation
    }
}
