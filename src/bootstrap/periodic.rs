use std::sync::Arc;

use crate::{
    commands::scheduler::CommandScheduler,
    managers::Managers,
    periodic_tasks::{
        self, BlockchainEventListenerDeps, ClaimRewardsDeps, CleanupDeps, DialPeersDeps,
        ParanetSyncDeps, ProvingDeps, SavePeerAddressesDeps, ShardingTableCheckDeps, SyncDeps,
    },
    services::Services,
};

pub(crate) fn build_periodic_tasks_deps(
    managers: &Managers,
    services: &Services,
    command_scheduler: &CommandScheduler,
) -> Arc<periodic_tasks::PeriodicTasksDeps> {
    let publish_tmp_dataset_store = Arc::new(managers.key_value_store.publish_tmp_dataset_store());
    let peer_address_store = Arc::new(managers.key_value_store.peer_address_store());

    Arc::new(periodic_tasks::PeriodicTasksDeps {
        dial_peers: DialPeersDeps {
            network_manager: Arc::clone(&managers.network),
            peer_service: Arc::clone(&services.peer_service),
        },
        cleanup: CleanupDeps {
            repository_manager: Arc::clone(&managers.repository),
            publish_tmp_dataset_store: Arc::clone(&publish_tmp_dataset_store),
            publish_operation_results: Arc::clone(&services.publish_store_operation),
            get_operation_results: Arc::clone(&services.get_operation),
            store_response_channels: Arc::clone(&services.response_channels.store),
            get_response_channels: Arc::clone(&services.response_channels.get),
            finality_response_channels: Arc::clone(&services.response_channels.finality),
            batch_get_response_channels: Arc::clone(&services.response_channels.batch_get),
        },
        save_peer_addresses: SavePeerAddressesDeps {
            peer_service: Arc::clone(&services.peer_service),
            peer_address_store: Arc::clone(&peer_address_store),
        },
        sharding_table_check: ShardingTableCheckDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            peer_service: Arc::clone(&services.peer_service),
        },
        blockchain_event_listener: BlockchainEventListenerDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            repository_manager: Arc::clone(&managers.repository),
            command_scheduler: command_scheduler.clone(),
        },
        claim_rewards: ClaimRewardsDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
        },
        proving: ProvingDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            repository_manager: Arc::clone(&managers.repository),
            triple_store_service: Arc::clone(&services.triple_store),
            network_manager: Arc::clone(&managers.network),
            assertion_validation_service: Arc::clone(&services.assertion_validation),
            peer_service: Arc::clone(&services.peer_service),
        },
        sync: SyncDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            repository_manager: Arc::clone(&managers.repository),
            triple_store_service: Arc::clone(&services.triple_store),
            network_manager: Arc::clone(&managers.network),
            assertion_validation_service: Arc::clone(&services.assertion_validation),
            peer_service: Arc::clone(&services.peer_service),
        },
        paranet_sync: ParanetSyncDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            repository_manager: Arc::clone(&managers.repository),
            triple_store_service: Arc::clone(&services.triple_store),
            get_fetch_service: Arc::clone(&services.get_fetch),
        },
    })
}
