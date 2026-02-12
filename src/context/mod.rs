use std::sync::Arc;

use crate::{
    commands::scheduler::CommandScheduler,
    managers::{
        Managers,
        key_value_store::PublishTmpDatasetStore,
        network::messages::{BatchGetAck, FinalityAck, GetAck, StoreAck},
    },
    services::{AssertionValidationService, PeerService, Services, TripleStoreService},
    state::ResponseChannels,
};

pub(crate) struct Context {
    command_scheduler: CommandScheduler,
    managers: Managers,
    services: Services,
}

impl Context {
    pub(crate) fn new(
        command_scheduler: CommandScheduler,
        managers: Managers,
        services: Services,
    ) -> Self {
        Self {
            command_scheduler,
            managers,
            services,
        }
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

    pub(crate) fn assertion_validation_service(&self) -> &Arc<AssertionValidationService> {
        &self.services.assertion_validation
    }

    pub(crate) fn peer_service(&self) -> &Arc<PeerService> {
        &self.services.peer_service
    }

    pub(crate) fn peer_address_store(&self) -> &Arc<crate::services::PeerAddressStore> {
        &self.services.peer_address_store
    }

    pub(crate) fn publish_tmp_dataset_store(&self) -> &Arc<PublishTmpDatasetStore> {
        &self.services.publish_tmp_dataset_store
    }

    // Response channel accessors
    pub(crate) fn store_response_channels(&self) -> &Arc<ResponseChannels<StoreAck>> {
        &self.services.response_channels.store
    }

    pub(crate) fn get_response_channels(&self) -> &Arc<ResponseChannels<GetAck>> {
        &self.services.response_channels.get
    }

    pub(crate) fn finality_response_channels(&self) -> &Arc<ResponseChannels<FinalityAck>> {
        &self.services.response_channels.finality
    }

    pub(crate) fn batch_get_response_channels(&self) -> &Arc<ResponseChannels<BatchGetAck>> {
        &self.services.response_channels.batch_get
    }

    // Operation status service accessors
    pub(crate) fn get_operation_status_service(
        &self,
    ) -> &Arc<crate::services::OperationStatusService<crate::operations::GetOperation>> {
        &self.services.get_operation
    }

    /// Publish polling status/results for store phase (signatures), not finality.
    pub(crate) fn publish_store_operation_status_service(
        &self,
    ) -> &Arc<crate::services::OperationStatusService<crate::operations::PublishStoreOperation>>
    {
        &self.services.publish_store_operation
    }
}
