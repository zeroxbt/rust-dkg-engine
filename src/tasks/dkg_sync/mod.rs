//! Sync queue processor with optional discovery worker.
//!
//! This module runs one long-lived queue processor composed of:
//! - Discovery: scans KC events, hydrates chain state, enqueues IDs
//! - Dispatcher + stages: drains queue and performs filter/fetch/insert

mod config;
mod deps;
mod discovery;
mod pipeline;
mod queue;
mod task;

use dkg_blockchain::BlockchainId;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

// Re-export public types
pub(crate) use config::{DkgSyncConfig, DkgSyncDiscoveryConfig, DkgSyncQueueProcessorConfig};
pub(crate) use deps::DkgSyncDeps;
pub(crate) use task::DkgSyncTask;

pub(crate) async fn run(
    deps: DkgSyncDeps,
    blockchain_ids: Vec<BlockchainId>,
    config: DkgSyncConfig,
    reorg_buffer_blocks: u64,
    shutdown: CancellationToken,
) {
    let mut set = JoinSet::new();
    for blockchain_id in blockchain_ids {
        let task_deps = deps.clone();
        let task_config = config.clone();
        let task_shutdown = shutdown.clone();
        set.spawn(async move {
            DkgSyncTask::new(task_deps, task_config, reorg_buffer_blocks)
                .run(&blockchain_id, task_shutdown)
                .await;
        });
    }

    while let Some(result) = set.join_next().await {
        match result {
            Ok(()) => {}
            Err(error) if error.is_panic() => {
                tracing::error!("DKG sync task panicked: {:?}", error);
            }
            Err(error) => {
                tracing::error!("DKG sync task failed: {:?}", error);
            }
        }
    }
}
