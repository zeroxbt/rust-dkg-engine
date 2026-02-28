mod deps;
mod registry;
mod runner;
pub(crate) mod tasks;
use std::sync::Arc;

use crate::tasks::sync_backfill::{SyncBackfillTask, SyncConfig};
pub(crate) use deps::{
    BlockchainEventListenerDeps, ClaimRewardsDeps, CleanupDeps, DialPeersDeps, ParanetSyncDeps,
    PeriodicTasksDeps, ProvingDeps, SavePeerAddressesDeps, ShardingTableCheckDeps,
    StateSnapshotDeps, SyncDeps,
};
use dkg_blockchain::BlockchainId;
use serde::{Deserialize, Serialize};
pub(crate) use tasks::sharding_table_check::seed_sharding_tables;
use tasks::{
    blockchain_event_listener::BlockchainEventListenerTask,
    claim_rewards::ClaimRewardsTask,
    cleanup::{CleanupConfig, CleanupTask},
    dial_peers::DialPeersTask,
    paranet_sync::{ParanetSyncConfig, ParanetSyncTask},
    proving::{ProvingConfig, ProvingTask},
    save_peer_addresses::SavePeerAddressesTask,
    sharding_table_check::ShardingTableCheckTask,
    state_snapshot::{StateSnapshotConfig, StateSnapshotTask},
};
use tokio_util::sync::CancellationToken;

use self::registry::{
    BlockchainPeriodicTask, GlobalPeriodicTask, spawn_blockchain_task, spawn_global_task,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PeriodicTasksConfig {
    pub cleanup: CleanupConfig,
    pub sync_backfill: SyncConfig,
    pub paranet_sync: ParanetSyncConfig,
    pub proving: ProvingConfig,
}

impl GlobalPeriodicTask for DialPeersTask {
    type Config = ();

    fn from_deps(deps: Arc<PeriodicTasksDeps>, _config: Self::Config) -> Self {
        Self::new(deps.dial_peers.clone())
    }

    fn run_task(self, shutdown: CancellationToken) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, shutdown)
    }
}

impl GlobalPeriodicTask for CleanupTask {
    type Config = CleanupConfig;

    fn from_deps(deps: Arc<PeriodicTasksDeps>, config: Self::Config) -> Self {
        Self::new(deps.cleanup.clone(), config)
    }

    fn run_task(self, shutdown: CancellationToken) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, shutdown)
    }
}

impl GlobalPeriodicTask for SavePeerAddressesTask {
    type Config = ();

    fn from_deps(deps: Arc<PeriodicTasksDeps>, _config: Self::Config) -> Self {
        Self::new(deps.save_peer_addresses.clone())
    }

    fn run_task(self, shutdown: CancellationToken) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, shutdown)
    }
}

impl GlobalPeriodicTask for StateSnapshotTask {
    type Config = StateSnapshotConfig;

    fn from_deps(deps: Arc<PeriodicTasksDeps>, config: Self::Config) -> Self {
        Self::new(deps.state_snapshot.clone(), config)
    }

    fn run_task(self, shutdown: CancellationToken) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, shutdown)
    }
}

impl BlockchainPeriodicTask for ShardingTableCheckTask {
    fn from_deps(deps: Arc<PeriodicTasksDeps>) -> Self {
        Self::new(deps.sharding_table_check.clone())
    }

    fn run_task(
        self,
        blockchain_id: &BlockchainId,
        shutdown: CancellationToken,
    ) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, blockchain_id, shutdown)
    }
}

impl BlockchainPeriodicTask for BlockchainEventListenerTask {
    fn from_deps(deps: Arc<PeriodicTasksDeps>) -> Self {
        Self::new(deps.blockchain_event_listener.clone())
    }

    fn run_task(
        self,
        blockchain_id: &BlockchainId,
        shutdown: CancellationToken,
    ) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, blockchain_id, shutdown)
    }
}

impl BlockchainPeriodicTask for ClaimRewardsTask {
    fn from_deps(deps: Arc<PeriodicTasksDeps>) -> Self {
        Self::new(deps.claim_rewards.clone())
    }

    fn run_task(
        self,
        blockchain_id: &BlockchainId,
        shutdown: CancellationToken,
    ) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, blockchain_id, shutdown)
    }
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
        cleanup: cleanup_config,
        sync_backfill: sync_backfill_config,
        paranet_sync: paranet_sync_config,
        proving: proving_config,
    } = periodic_tasks_config;

    let mut set = tokio::task::JoinSet::new();
    let cleanup_enabled = cleanup_config.enabled;
    let sync_backfill_enabled = sync_backfill_config.enabled;
    let paranet_sync_enabled =
        paranet_sync_config.enabled && !paranet_sync_config.sync_paranets.is_empty();
    let state_snapshot_enabled = sync_backfill_config.enabled && metrics_enabled;

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
                max_retry_attempts: sync_backfill_config.max_retry_attempts,
            },
        );
    }

    // Per-blockchain tasks/workloads
    for blockchain_id in blockchain_ids {
        if sync_backfill_enabled {
            let deps = Arc::clone(&deps);
            let shutdown = shutdown.clone();
            let blockchain_id = blockchain_id.clone();
            let config = sync_backfill_config.clone();
            set.spawn(async move {
                SyncBackfillTask::new(deps.sync_backfill.clone(), config)
                    .run(&blockchain_id, shutdown)
                    .await;
            });
        }

        // Per-blockchain paranet sync task has dedicated config and does not fit
        // the generic BlockchainPeriodicTask registry helper.
        if paranet_sync_enabled {
            let deps = Arc::clone(&deps);
            let shutdown = shutdown.clone();
            let blockchain_id = blockchain_id.clone();
            let config = paranet_sync_config.clone();
            set.spawn(async move {
                ParanetSyncTask::new(deps.paranet_sync.clone(), config)
                    .run(&blockchain_id, shutdown)
                    .await;
            });
        }

        spawn_registered_blockchain_tasks!(
            &mut set,
            &deps,
            &shutdown,
            &blockchain_id,
            ShardingTableCheckTask,
            BlockchainEventListenerTask,
            ClaimRewardsTask,
        );

        if proving_config.enabled {
            let deps = Arc::clone(&deps);
            let shutdown = shutdown.clone();
            let blockchain_id = blockchain_id.clone();
            set.spawn(async move {
                ProvingTask::new(deps.proving.clone())
                    .run(&blockchain_id, shutdown)
                    .await
            });
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
