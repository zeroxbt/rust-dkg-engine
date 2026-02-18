use std::sync::Arc;

use crate::{
    bootstrap::ApplicationDeps,
    commands::scheduler::CommandScheduler,
    managers::Managers,
    node_state::NodeState,
    periodic_tasks::{
        self, BlockchainEventListenerDeps, ClaimRewardsDeps, CleanupDeps, DialPeersDeps,
        ParanetSyncDeps, ProvingDeps, SavePeerAddressesDeps, ShardingTableCheckDeps, SyncDeps,
    },
};

pub(crate) fn build_periodic_tasks_deps(
    managers: &Managers,
    node_state: &NodeState,
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
            peer_registry: Arc::clone(&node_state.peer_registry),
        },
        cleanup: CleanupDeps {
            operation_repository,
            finality_status_repository,
            proof_challenge_repository: proof_challenge_repository.clone(),
            publish_tmp_dataset_store: Arc::clone(&publish_tmp_dataset_store),
            publish_operation_tracking: Arc::clone(&application.publish_store_operation_tracking),
            get_operation_tracking: Arc::clone(&application.get_operation_tracking),
            store_response_channels: Arc::clone(&node_state.store_response_channels),
            get_response_channels: Arc::clone(&node_state.get_response_channels),
            finality_response_channels: Arc::clone(&node_state.finality_response_channels),
            batch_get_response_channels: Arc::clone(&node_state.batch_get_response_channels),
        },
        save_peer_addresses: SavePeerAddressesDeps {
            peer_registry: Arc::clone(&node_state.peer_registry),
            peer_address_store: Arc::clone(&peer_address_store),
        },
        sharding_table_check: ShardingTableCheckDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            peer_registry: Arc::clone(&node_state.peer_registry),
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
            network_manager: Arc::clone(&managers.network),
            peer_registry: Arc::clone(&node_state.peer_registry),
            proof_challenge_repository,
            assertion_retrieval: Arc::clone(&application.assertion_retrieval),
        },
        sync: SyncDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            kc_sync_repository,
            triple_store_assertions: Arc::clone(&application.triple_store_assertions),
            network_manager: Arc::clone(&managers.network),
            assertion_validation: Arc::clone(&application.assertion_validation),
            peer_registry: Arc::clone(&node_state.peer_registry),
        },
        paranet_sync: ParanetSyncDeps {
            blockchain_manager: Arc::clone(&managers.blockchain),
            paranet_kc_sync_repository,
            triple_store_assertions: Arc::clone(&application.triple_store_assertions),
            get_assertion_use_case: Arc::clone(&application.get_assertion_use_case),
        },
    })
}
