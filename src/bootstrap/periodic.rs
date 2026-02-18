use std::sync::Arc;

use crate::{
    bootstrap::ApplicationDeps,
    commands::scheduler::CommandScheduler,
    managers::Managers,
    periodic_tasks::{
        self, BlockchainEventListenerDeps, ClaimRewardsDeps, CleanupDeps, DialPeersDeps,
        ParanetSyncDeps, ProvingDeps, SavePeerAddressesDeps, ShardingTableCheckDeps, SyncDeps,
    },
    runtime_state::RuntimeState,
};

pub(crate) fn build_periodic_tasks_deps(
    managers: &Managers,
    runtime_state: &RuntimeState,
    application: &ApplicationDeps,
    command_scheduler: &CommandScheduler,
) -> Arc<periodic_tasks::PeriodicTasksDeps> {
    let publish_tmp_dataset_store = Arc::new(managers.key_value_store.publish_tmp_dataset_store());
    let peer_address_store = Arc::new(managers.key_value_store.peer_address_store());
    let operation_repository = managers.repository.operation_repository();
    let finality_status_repository = managers.repository.finality_status_repository();
    let proof_challenge_repository = managers.repository.proof_challenge_repository();
    let blockchain_repository = managers.repository.blockchain_repository();
    let kc_sync_repository = managers.repository.kc_sync_repository();
    let paranet_kc_sync_repository = managers.repository.paranet_kc_sync_repository();

    Arc::new(periodic_tasks::PeriodicTasksDeps {
        dial_peers: DialPeersDeps {
            network_manager: Arc::clone(&managers.network),
            peer_directory: Arc::clone(&runtime_state.peer_directory),
        },
        cleanup: CleanupDeps {
            operation_repository,
            finality_status_repository,
            proof_challenge_repository: proof_challenge_repository.clone(),
            publish_tmp_dataset_store: Arc::clone(&publish_tmp_dataset_store),
            publish_operation_tracking: Arc::clone(&application.publish_store_operation_tracking),
            get_operation_tracking: Arc::clone(&application.get_operation_tracking),
            store_response_channels: Arc::clone(&runtime_state.response_channels.store),
            get_response_channels: Arc::clone(&runtime_state.response_channels.get),
            finality_response_channels: Arc::clone(&runtime_state.response_channels.finality),
            batch_get_response_channels: Arc::clone(&runtime_state.response_channels.batch_get),
        },
        save_peer_addresses: SavePeerAddressesDeps {
            peer_directory: Arc::clone(&runtime_state.peer_directory),
            peer_address_store: Arc::clone(&peer_address_store),
        },
        sharding_table_check: ShardingTableCheckDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            peer_directory: Arc::clone(&runtime_state.peer_directory),
        },
        blockchain_event_listener: BlockchainEventListenerDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            blockchain_repository,
            command_scheduler: command_scheduler.clone(),
        },
        claim_rewards: ClaimRewardsDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
        },
        proving: ProvingDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            proof_challenge_repository,
            triple_store_assertions: Arc::clone(&application.triple_store_assertions),
            network_manager: Arc::clone(&managers.network),
            assertion_validation: Arc::clone(&application.assertion_validation),
            peer_directory: Arc::clone(&runtime_state.peer_directory),
        },
        sync: SyncDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            kc_sync_repository,
            triple_store_assertions: Arc::clone(&application.triple_store_assertions),
            network_manager: Arc::clone(&managers.network),
            assertion_validation: Arc::clone(&application.assertion_validation),
            peer_directory: Arc::clone(&runtime_state.peer_directory),
        },
        paranet_sync: ParanetSyncDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            paranet_kc_sync_repository,
            triple_store_assertions: Arc::clone(&application.triple_store_assertions),
            get_assertion_use_case: Arc::clone(&application.get_assertion_use_case),
        },
    })
}
