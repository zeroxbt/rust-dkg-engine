use std::sync::Arc;

use dkg_network::{BatchGetAck, FinalityAck, GetAck, StoreAck};

use crate::{
    commands::scheduler::CommandScheduler,
    managers::Managers,
    services::{PeerService, Services},
    state::ResponseChannels,
};

mod commands;
mod paranet_sync;
mod periodic;
mod sync;
pub(crate) use commands::{
    HandleBatchGetRequestDeps, HandleGetRequestDeps, HandlePublishFinalityRequestDeps,
    HandlePublishStoreRequestDeps, SendGetRequestsDeps, SendPublishFinalityRequestDeps,
    SendPublishStoreRequestsDeps,
};
pub(crate) use paranet_sync::ParanetSyncDeps;
pub(crate) use periodic::{
    BlockchainEventListenerDeps, ClaimRewardsDeps, CleanupDeps, DialPeersDeps, ProvingDeps,
    SavePeerAddressesDeps, ShardingTableCheckDeps,
};
pub(crate) use sync::SyncDeps;

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
    pub(crate) fn repository_manager(&self) -> &Arc<dkg_repository::RepositoryManager> {
        &self.managers.repository
    }

    pub(crate) fn network_manager(&self) -> &Arc<dkg_network::NetworkManager> {
        &self.managers.network
    }

    pub(crate) fn blockchain_manager(&self) -> &Arc<dkg_blockchain::BlockchainManager> {
        &self.managers.blockchain
    }

    // Service accessors
    pub(crate) fn peer_service(&self) -> &Arc<PeerService> {
        &self.services.peer_service
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

    pub(crate) fn sync_deps(&self) -> SyncDeps {
        SyncDeps {
            blockchain_manager: Arc::clone(&self.managers.blockchain),
            repository_manager: Arc::clone(&self.managers.repository),
            triple_store_service: Arc::clone(&self.services.triple_store),
            network_manager: Arc::clone(&self.managers.network),
            assertion_validation_service: Arc::clone(&self.services.assertion_validation),
            peer_service: Arc::clone(&self.services.peer_service),
        }
    }

    pub(crate) fn paranet_sync_deps(&self) -> ParanetSyncDeps {
        ParanetSyncDeps {
            blockchain_manager: Arc::clone(&self.managers.blockchain),
            repository_manager: Arc::clone(&self.managers.repository),
            triple_store_service: Arc::clone(&self.services.triple_store),
            get_fetch_service: Arc::clone(&self.services.get_fetch),
        }
    }

    pub(crate) fn dial_peers_deps(&self) -> DialPeersDeps {
        DialPeersDeps {
            network_manager: Arc::clone(&self.managers.network),
            peer_service: Arc::clone(&self.services.peer_service),
        }
    }

    pub(crate) fn save_peer_addresses_deps(&self) -> SavePeerAddressesDeps {
        SavePeerAddressesDeps {
            peer_service: Arc::clone(&self.services.peer_service),
            peer_address_store: Arc::clone(&self.services.peer_address_store),
        }
    }

    pub(crate) fn claim_rewards_deps(&self) -> ClaimRewardsDeps {
        ClaimRewardsDeps {
            blockchain_manager: Arc::clone(&self.managers.blockchain),
        }
    }

    pub(crate) fn cleanup_deps(&self) -> CleanupDeps {
        CleanupDeps {
            repository_manager: Arc::clone(&self.managers.repository),
            publish_tmp_dataset_store: Arc::clone(&self.services.publish_tmp_dataset_store),
            publish_operation_results: Arc::clone(&self.services.publish_store_operation),
            get_operation_results: Arc::clone(&self.services.get_operation),
            store_response_channels: Arc::clone(&self.services.response_channels.store),
            get_response_channels: Arc::clone(&self.services.response_channels.get),
            finality_response_channels: Arc::clone(&self.services.response_channels.finality),
            batch_get_response_channels: Arc::clone(&self.services.response_channels.batch_get),
        }
    }

    pub(crate) fn sharding_table_check_deps(&self) -> ShardingTableCheckDeps {
        ShardingTableCheckDeps {
            blockchain_manager: Arc::clone(&self.managers.blockchain),
            peer_service: Arc::clone(&self.services.peer_service),
        }
    }

    pub(crate) fn blockchain_event_listener_deps(&self) -> BlockchainEventListenerDeps {
        BlockchainEventListenerDeps {
            blockchain_manager: Arc::clone(&self.managers.blockchain),
            repository_manager: Arc::clone(&self.managers.repository),
            command_scheduler: self.command_scheduler.clone(),
        }
    }

    pub(crate) fn proving_deps(&self) -> ProvingDeps {
        ProvingDeps {
            blockchain_manager: Arc::clone(&self.managers.blockchain),
            repository_manager: Arc::clone(&self.managers.repository),
            triple_store_service: Arc::clone(&self.services.triple_store),
            network_manager: Arc::clone(&self.managers.network),
            assertion_validation_service: Arc::clone(&self.services.assertion_validation),
            peer_service: Arc::clone(&self.services.peer_service),
        }
    }

    pub(crate) fn send_publish_store_requests_deps(&self) -> SendPublishStoreRequestsDeps {
        SendPublishStoreRequestsDeps {
            network_manager: Arc::clone(&self.managers.network),
            peer_service: Arc::clone(&self.services.peer_service),
            blockchain_manager: Arc::clone(&self.managers.blockchain),
            publish_store_operation_status_service: Arc::clone(
                &self.services.publish_store_operation,
            ),
            publish_tmp_dataset_store: Arc::clone(&self.services.publish_tmp_dataset_store),
        }
    }

    pub(crate) fn handle_publish_store_request_deps(&self) -> HandlePublishStoreRequestDeps {
        HandlePublishStoreRequestDeps {
            network_manager: Arc::clone(&self.managers.network),
            blockchain_manager: Arc::clone(&self.managers.blockchain),
            peer_service: Arc::clone(&self.services.peer_service),
            store_response_channels: Arc::clone(&self.services.response_channels.store),
            publish_tmp_dataset_store: Arc::clone(&self.services.publish_tmp_dataset_store),
        }
    }

    pub(crate) fn send_publish_finality_request_deps(&self) -> SendPublishFinalityRequestDeps {
        SendPublishFinalityRequestDeps {
            repository_manager: Arc::clone(&self.managers.repository),
            network_manager: Arc::clone(&self.managers.network),
            peer_service: Arc::clone(&self.services.peer_service),
            blockchain_manager: Arc::clone(&self.managers.blockchain),
            publish_tmp_dataset_store: Arc::clone(&self.services.publish_tmp_dataset_store),
            triple_store_service: Arc::clone(&self.services.triple_store),
        }
    }

    pub(crate) fn handle_publish_finality_request_deps(&self) -> HandlePublishFinalityRequestDeps {
        HandlePublishFinalityRequestDeps {
            repository_manager: Arc::clone(&self.managers.repository),
            network_manager: Arc::clone(&self.managers.network),
            finality_response_channels: Arc::clone(&self.services.response_channels.finality),
        }
    }

    pub(crate) fn send_get_requests_deps(&self) -> SendGetRequestsDeps {
        SendGetRequestsDeps {
            get_operation_status_service: Arc::clone(&self.services.get_operation),
            get_fetch_service: Arc::clone(&self.services.get_fetch),
        }
    }

    pub(crate) fn handle_get_request_deps(&self) -> HandleGetRequestDeps {
        HandleGetRequestDeps {
            network_manager: Arc::clone(&self.managers.network),
            triple_store_service: Arc::clone(&self.services.triple_store),
            peer_service: Arc::clone(&self.services.peer_service),
            get_response_channels: Arc::clone(&self.services.response_channels.get),
            blockchain_manager: Arc::clone(&self.managers.blockchain),
        }
    }

    pub(crate) fn handle_batch_get_request_deps(&self) -> HandleBatchGetRequestDeps {
        HandleBatchGetRequestDeps {
            network_manager: Arc::clone(&self.managers.network),
            triple_store_service: Arc::clone(&self.services.triple_store),
            peer_service: Arc::clone(&self.services.peer_service),
            batch_get_response_channels: Arc::clone(&self.services.response_channels.batch_get),
        }
    }
}
