mod registry;
mod runner;
pub(crate) mod tasks;

use std::sync::Arc;

pub(crate) use tasks::sharding_table_check::seed_sharding_tables;
use tasks::{
    blockchain_event_listener::BlockchainEventListenerTask,
    claim_rewards::ClaimRewardsTask,
    cleanup::{CleanupConfig, CleanupTask},
    dial_peers::DialPeersTask,
    proving::ProvingTask,
    save_peer_addresses::SavePeerAddressesTask,
    sharding_table_check::ShardingTableCheckTask,
    sync::SyncTask,
};
use tokio_util::sync::CancellationToken;

use self::registry::{
    BlockchainPeriodicTask, GlobalPeriodicTask, spawn_blockchain_task, spawn_global_task,
};
use crate::{context::Context, managers::blockchain::BlockchainId};

macro_rules! impl_global_periodic_task {
    ($task:ty) => {
        impl GlobalPeriodicTask for $task {
            type Config = ();

            fn from_context(context: Arc<Context>, _config: Self::Config) -> Self {
                <$task>::new(context)
            }

            fn run_task(
                self,
                shutdown: CancellationToken,
            ) -> impl std::future::Future<Output = ()> + Send {
                <$task>::run(self, shutdown)
            }
        }
    };
    ($task:ty, $config:ty) => {
        impl GlobalPeriodicTask for $task {
            type Config = $config;

            fn from_context(context: Arc<Context>, config: Self::Config) -> Self {
                <$task>::new(context, config)
            }

            fn run_task(
                self,
                shutdown: CancellationToken,
            ) -> impl std::future::Future<Output = ()> + Send {
                <$task>::run(self, shutdown)
            }
        }
    };
}

macro_rules! impl_blockchain_periodic_task {
    ( $( $task:ty ),+ $(,)? ) => {
        $(
            impl BlockchainPeriodicTask for $task {
                fn from_context(context: Arc<Context>) -> Self {
                    <$task>::new(context)
                }

                fn run_task(
                    self,
                    blockchain_id: &BlockchainId,
                    shutdown: CancellationToken,
                ) -> impl std::future::Future<Output = ()> + Send {
                    <$task>::run(self, blockchain_id, shutdown)
                }
            }
        )+
    };
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

impl_global_periodic_task!(DialPeersTask);
impl_global_periodic_task!(CleanupTask, CleanupConfig);
impl_global_periodic_task!(SavePeerAddressesTask);

impl_blockchain_periodic_task!(
    ShardingTableCheckTask,
    BlockchainEventListenerTask,
    SyncTask,
    ProvingTask,
    ClaimRewardsTask,
);

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
        spawn_registered_blockchain_tasks!(
            &mut set,
            &context,
            &shutdown,
            &blockchain_id,
            ShardingTableCheckTask,
            BlockchainEventListenerTask,
            SyncTask,
            ProvingTask,
            ClaimRewardsTask,
        );
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
