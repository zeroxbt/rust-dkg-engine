mod deps;
mod registry;
mod runner;
pub(crate) mod tasks;
use std::sync::Arc;

pub(crate) use deps::PeriodicTasksDeps;
use dkg_blockchain::BlockchainId;
use serde::{Deserialize, Serialize};
pub(crate) use tasks::{
    blockchain_admin_events::BlockchainAdminEventsDeps,
    claim_rewards::ClaimRewardsDeps,
    cleanup::CleanupDeps,
    dial_peers::DialPeersDeps,
    kc_reconciliation::KcReconciliationDeps,
    paranet_sync::ParanetSyncDeps,
    proving::ProvingDeps,
    save_peer_addresses::SavePeerAddressesDeps,
    sharding_table_check::{ShardingTableCheckDeps, seed_sharding_tables},
    state_snapshot::StateSnapshotDeps,
};
use tasks::{
    blockchain_admin_events::{BlockchainAdminEventsConfig, BlockchainAdminEventsTask},
    claim_rewards::ClaimRewardsTask,
    cleanup::{CleanupConfig, CleanupTask},
    dial_peers::DialPeersTask,
    kc_reconciliation::{KcReconciliationConfig, KcReconciliationTask},
    paranet_sync::{ParanetSyncConfig, ParanetSyncTask},
    proving::{ProvingConfig, ProvingTask},
    save_peer_addresses::SavePeerAddressesTask,
    sharding_table_check::ShardingTableCheckTask,
    state_snapshot::{StateSnapshotConfig, StateSnapshotTask},
};
use tokio_util::sync::CancellationToken;

use self::registry::{spawn_blockchain_task, spawn_configured_blockchain_task, spawn_global_task};
use crate::tasks::dkg_sync::DkgSyncConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PeriodicTasksConfig {
    pub reorg_buffer_blocks: u64,
    pub cleanup: CleanupConfig,
    pub blockchain_admin_events: BlockchainAdminEventsConfig,
    pub dkg_sync: DkgSyncConfig,
    pub kc_reconciliation: KcReconciliationConfig,
    pub paranet_sync: ParanetSyncConfig,
    pub proving: ProvingConfig,
}

macro_rules! spawn_registered_global_tasks {
    ($set:expr, $deps:expr, $shutdown:expr, $( $task:ty => $config:expr ),+ $(,)? ) => {
        $(
            spawn_global_task::<$task>($set, $deps, $shutdown, $config);
        )+
    };
}

macro_rules! spawn_registered_blockchain_tasks {
    ($set:expr, $deps:expr, $shutdown:expr, $blockchain_id:expr, $( $task:ty ),+ $(,)? ) => {
        $(
            spawn_blockchain_task::<$task>($set, $deps, $shutdown, $blockchain_id);
        )+
    };
}

/// Spawn all periodic tasks and wait for them to complete.
///
/// Each task runs in its own tokio task with an independent loop.
/// Uses `JoinSet` to log panics immediately as they happen rather
/// than waiting for all tasks to finish (as `join_all` would).
///
/// Under normal operation, tasks only exit during shutdown.
pub(crate) async fn run(
    deps: Arc<PeriodicTasksDeps>,
    blockchain_ids: Vec<BlockchainId>,
    periodic_tasks_config: PeriodicTasksConfig,
    metrics_enabled: bool,
    shutdown: CancellationToken,
) {
    let PeriodicTasksConfig {
        reorg_buffer_blocks,
        cleanup: cleanup_config,
        blockchain_admin_events: blockchain_admin_events_config,
        dkg_sync: dkg_sync_config,
        kc_reconciliation: kc_reconciliation_config,
        paranet_sync: paranet_sync_config,
        proving: proving_config,
    } = periodic_tasks_config;

    let mut set = tokio::task::JoinSet::new();
    let cleanup_enabled = cleanup_config.enabled;
    let kc_reconciliation_enabled = kc_reconciliation_config.enabled;
    let paranet_sync_enabled =
        paranet_sync_config.enabled && !paranet_sync_config.sync_paranets.is_empty();
    let state_snapshot_enabled = metrics_enabled;

    // Global periodic tasks
    spawn_registered_global_tasks!(
        &mut set,
        &deps,
        &shutdown,
        DialPeersTask => (),
        SavePeerAddressesTask => (),
    );
    if cleanup_enabled {
        spawn_global_task::<CleanupTask>(&mut set, &deps, &shutdown, cleanup_config);
    }
    if state_snapshot_enabled {
        spawn_global_task::<StateSnapshotTask>(
            &mut set,
            &deps,
            &shutdown,
            StateSnapshotConfig {
                interval_secs: 60,
                blockchain_ids: blockchain_ids.clone(),
                max_retry_attempts: dkg_sync_config.queue_processor.max_retry_attempts,
            },
        );
    }

    // Per-blockchain tasks/workloads
    for blockchain_id in blockchain_ids {
        let reorg_buffer_blocks = reorg_buffer_blocks.max(1);

        if paranet_sync_enabled {
            spawn_configured_blockchain_task::<ParanetSyncTask>(
                &mut set,
                &deps,
                &shutdown,
                &blockchain_id,
                paranet_sync_config.clone(),
            );
        }

        if kc_reconciliation_enabled {
            spawn_configured_blockchain_task::<KcReconciliationTask>(
                &mut set,
                &deps,
                &shutdown,
                &blockchain_id,
                kc_reconciliation_config.clone(),
            );
        }

        spawn_configured_blockchain_task::<BlockchainAdminEventsTask>(
            &mut set,
            &deps,
            &shutdown,
            &blockchain_id,
            (blockchain_admin_events_config.clone(), reorg_buffer_blocks),
        );

        spawn_registered_blockchain_tasks!(
            &mut set,
            &deps,
            &shutdown,
            &blockchain_id,
            ShardingTableCheckTask,
            ClaimRewardsTask,
        );

        if proving_config.enabled {
            spawn_blockchain_task::<ProvingTask>(&mut set, &deps, &shutdown, &blockchain_id);
        }
    }

    // Wait for tasks — log panics immediately as they happen.
    // Under normal operation, tasks only exit during shutdown (they loop forever).
    while let Some(result) = set.join_next().await {
        match result {
            Ok(()) => {
                // Task exited normally (shutdown)
            }
            Err(e) if e.is_panic() => {
                tracing::error!("Periodic task panicked: {:?}", e);
            }
            Err(e) => {
                tracing::error!("Periodic task failed: {:?}", e);
            }
        }
    }
}
