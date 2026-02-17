mod registry;
mod runner;
pub(crate) mod tasks;
use std::sync::Arc;

use dkg_blockchain::BlockchainId;
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
    sync::{SyncConfig, SyncTask},
};
use tokio_util::sync::CancellationToken;

use self::registry::{
    BlockchainPeriodicTask, GlobalPeriodicTask, spawn_blockchain_task, spawn_global_task,
};
use crate::context::Context;

impl GlobalPeriodicTask for DialPeersTask {
    type Config = ();

    fn from_context(context: Arc<Context>, _config: Self::Config) -> Self {
        Self::new(context.dial_peers_deps())
    }

    fn run_task(self, shutdown: CancellationToken) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, shutdown)
    }
}

impl GlobalPeriodicTask for CleanupTask {
    type Config = CleanupConfig;

    fn from_context(context: Arc<Context>, config: Self::Config) -> Self {
        Self::new(context.cleanup_deps(), config)
    }

    fn run_task(self, shutdown: CancellationToken) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, shutdown)
    }
}

impl GlobalPeriodicTask for SavePeerAddressesTask {
    type Config = ();

    fn from_context(context: Arc<Context>, _config: Self::Config) -> Self {
        Self::new(context.save_peer_addresses_deps())
    }

    fn run_task(self, shutdown: CancellationToken) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, shutdown)
    }
}

impl BlockchainPeriodicTask for ShardingTableCheckTask {
    fn from_context(context: Arc<Context>) -> Self {
        Self::new(context.sharding_table_check_deps())
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
    fn from_context(context: Arc<Context>) -> Self {
        Self::new(context.blockchain_event_listener_deps())
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
    fn from_context(context: Arc<Context>) -> Self {
        Self::new(context.claim_rewards_deps())
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
    ($set:expr, $context:expr, $shutdown:expr, $( $task:ty => $config:expr ),+ $(,)? ) => {
        $(
            spawn_global_task::<$task>($set, $context, $shutdown, $config);
        )+
    };
}

macro_rules! spawn_registered_blockchain_tasks {
    ($set:expr, $context:expr, $shutdown:expr, $blockchain_id:expr, $( $task:ty ),+ $(,)? ) => {
        $(
            spawn_blockchain_task::<$task>($set, $context, $shutdown, $blockchain_id);
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
pub(crate) async fn run_all(
    context: Arc<Context>,
    cleanup_config: CleanupConfig,
    sync_config: SyncConfig,
    paranet_sync_config: ParanetSyncConfig,
    proving_config: ProvingConfig,
    shutdown: CancellationToken,
) {
    let mut set = tokio::task::JoinSet::new();

    // Global periodic tasks
    spawn_registered_global_tasks!(
        &mut set,
        &context,
        &shutdown,
        DialPeersTask => (),
        CleanupTask => cleanup_config,
        SavePeerAddressesTask => (),
    );

    // Collect blockchain IDs for periodic tasks
    let blockchain_ids: Vec<_> = context
        .blockchain_manager()
        .get_blockchain_ids()
        .into_iter()
        .cloned()
        .collect();

    // Per-blockchain periodic tasks
    for blockchain_id in blockchain_ids {
        // Per-blockchain sync task has dedicated config and does not fit
        // the generic BlockchainPeriodicTask registry helper.
        {
            let ctx = Arc::clone(&context);
            let shutdown = shutdown.clone();
            let blockchain_id = blockchain_id.clone();
            let config = sync_config.clone();
            set.spawn(async move {
                SyncTask::new(ctx.sync_deps(), config)
                    .run(&blockchain_id, shutdown)
                    .await;
            });
        }

        // Per-blockchain paranet sync task has dedicated config and does not fit
        // the generic BlockchainPeriodicTask registry helper.
        {
            let ctx = Arc::clone(&context);
            let shutdown = shutdown.clone();
            let blockchain_id = blockchain_id.clone();
            let config = paranet_sync_config.clone();
            set.spawn(async move {
                ParanetSyncTask::new(ctx.paranet_sync_deps(), config)
                    .run(&blockchain_id, shutdown)
                    .await;
            });
        }

        spawn_registered_blockchain_tasks!(
            &mut set,
            &context,
            &shutdown,
            &blockchain_id,
            ShardingTableCheckTask,
            BlockchainEventListenerTask,
            ClaimRewardsTask,
        );

        if proving_config.enabled {
            let ctx = Arc::clone(&context);
            let shutdown = shutdown.clone();
            let blockchain_id = blockchain_id.clone();
            set.spawn(async move {
                ProvingTask::new(ctx.proving_deps())
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
