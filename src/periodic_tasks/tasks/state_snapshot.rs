use std::{sync::Arc, time::Duration};

use chrono::Utc;
use dkg_blockchain::BlockchainId;
use dkg_network::{
    PROTOCOL_NAME_BATCH_GET, PROTOCOL_NAME_GET, STREAM_PROTOCOL_BATCH_GET, STREAM_PROTOCOL_GET,
};
use dkg_observability as observability;
use dkg_repository::{KcChainMetadataRepository, KcSyncRepository};
use tokio_util::sync::CancellationToken;

use crate::{
    node_state::PeerRegistry, periodic_tasks::StateSnapshotDeps,
    periodic_tasks::runner::run_with_shutdown,
};

#[derive(Debug, Clone)]
pub(crate) struct StateSnapshotConfig {
    pub interval_secs: u64,
    pub blockchain_ids: Vec<BlockchainId>,
    pub max_retry_attempts: u32,
}

pub(crate) struct StateSnapshotTask {
    config: StateSnapshotConfig,
    kc_sync_repository: KcSyncRepository,
    kc_chain_metadata_repository: KcChainMetadataRepository,
    peer_registry: Arc<PeerRegistry>,
}

impl StateSnapshotTask {
    pub(crate) fn new(deps: StateSnapshotDeps, config: StateSnapshotConfig) -> Self {
        Self {
            config,
            kc_sync_repository: deps.kc_sync_repository,
            kc_chain_metadata_repository: deps.kc_chain_metadata_repository,
            peer_registry: deps.peer_registry,
        }
    }

    pub(crate) async fn run(self, shutdown: CancellationToken) {
        run_with_shutdown("state_snapshot", shutdown, || self.execute()).await;
    }

    #[tracing::instrument(name = "periodic_tasks.state_snapshot", skip(self))]
    async fn execute(&self) -> Duration {
        let interval = Duration::from_secs(self.config.interval_secs.max(5));

        let now_ts = Utc::now().timestamp();
        observability::record_peer_registry_discovery_backoff(
            self.peer_registry.discovery_backoff_active_count(),
        );
        if let Some(snapshot) = collect_process_fd_snapshot() {
            observability::record_process_fd_snapshot(
                snapshot.total,
                snapshot.sockets,
                snapshot.pipes,
                snapshot.anon_inodes,
                snapshot.files,
                snapshot.other,
            );
        }

        for blockchain_id in &self.config.blockchain_ids {
            let blockchain_label = blockchain_id.as_str();

            self.collect_queue_snapshot(blockchain_label, now_ts).await;
            self.collect_sync_totals_snapshot(blockchain_label).await;
            self.collect_peer_registry_snapshot(blockchain_id);
        }

        interval
    }

    async fn collect_queue_snapshot(&self, blockchain_id: &str, now_ts: i64) {
        let queue_total = match self
            .kc_sync_repository
            .count_queue_total_for_blockchain(blockchain_id)
            .await
        {
            Ok(v) => v,
            Err(error) => {
                tracing::warn!(
                    blockchain_id,
                    error = %error,
                    "Failed to count total sync queue rows"
                );
                return;
            }
        };

        let queue_due = match self
            .kc_sync_repository
            .count_queue_due_for_blockchain(blockchain_id, now_ts, self.config.max_retry_attempts)
            .await
        {
            Ok(v) => v,
            Err(error) => {
                tracing::warn!(
                    blockchain_id,
                    error = %error,
                    "Failed to count due sync queue rows"
                );
                return;
            }
        };

        let queue_retrying = match self
            .kc_sync_repository
            .count_queue_retrying_for_blockchain(
                blockchain_id,
                now_ts,
                self.config.max_retry_attempts,
            )
            .await
        {
            Ok(v) => v,
            Err(error) => {
                tracing::warn!(
                    blockchain_id,
                    error = %error,
                    "Failed to count retrying sync queue rows"
                );
                return;
            }
        };

        let oldest_due_age_secs = match self
            .kc_sync_repository
            .oldest_due_created_at_for_blockchain(
                blockchain_id,
                now_ts,
                self.config.max_retry_attempts,
            )
            .await
        {
            Ok(Some(created_at)) => now_ts.saturating_sub(created_at).max(0) as u64,
            Ok(None) => 0,
            Err(error) => {
                tracing::warn!(
                    blockchain_id,
                    error = %error,
                    "Failed to read oldest due sync queue row"
                );
                0
            }
        };

        let progress_tracked_contracts = match self
            .kc_sync_repository
            .count_progress_contracts_for_blockchain(blockchain_id)
            .await
        {
            Ok(v) => v,
            Err(error) => {
                tracing::warn!(
                    blockchain_id,
                    error = %error,
                    "Failed to count sync progress contracts"
                );
                0
            }
        };

        let progress_last_update_age_secs = match self
            .kc_sync_repository
            .latest_progress_updated_at_for_blockchain(blockchain_id)
            .await
        {
            Ok(Some(updated_at)) => now_ts.saturating_sub(updated_at).max(0) as u64,
            Ok(None) => 0,
            Err(error) => {
                tracing::warn!(
                    blockchain_id,
                    error = %error,
                    "Failed to read latest sync progress update"
                );
                0
            }
        };

        observability::record_sync_queue_snapshot(
            blockchain_id,
            queue_total,
            queue_due,
            queue_retrying,
            oldest_due_age_secs,
            progress_tracked_contracts,
            progress_last_update_age_secs,
        );
    }

    async fn collect_sync_totals_snapshot(&self, blockchain_id: &str) {
        let metadata_kcs_total = match self
            .kc_chain_metadata_repository
            .count_core_metadata_for_blockchain(blockchain_id)
            .await
        {
            Ok(v) => v,
            Err(error) => {
                tracing::warn!(
                    blockchain_id,
                    error = %error,
                    "Failed to count KC core metadata rows"
                );
                0
            }
        };

        let metadata_backfill_kcs_total = match self
            .kc_chain_metadata_repository
            .count_core_metadata_for_blockchain_by_source(blockchain_id, "sync_metadata_backfill")
            .await
        {
            Ok(v) => v,
            Err(error) => {
                tracing::warn!(
                    blockchain_id,
                    error = %error,
                    "Failed to count KC core metadata rows from sync_metadata_backfill source"
                );
                0
            }
        };

        observability::record_sync_metadata_total_snapshot(blockchain_id, metadata_kcs_total);
        observability::record_sync_metadata_backfill_total_snapshot(
            blockchain_id,
            metadata_backfill_kcs_total,
        );
    }

    fn collect_peer_registry_snapshot(&self, blockchain_id: &BlockchainId) {
        let blockchain_label = blockchain_id.as_str();
        observability::record_peer_registry_population(
            blockchain_label,
            self.peer_registry.known_peer_count(),
            self.peer_registry.shard_peer_count(blockchain_id),
            self.peer_registry
                .identified_shard_peer_count(blockchain_id),
        );

        let batch_get_capable = self
            .peer_registry
            .protocol_capable_shard_peer_count(blockchain_id, STREAM_PROTOCOL_BATCH_GET);
        observability::record_peer_registry_protocol_capable_shard_peers(
            blockchain_label,
            PROTOCOL_NAME_BATCH_GET,
            batch_get_capable,
        );

        let get_capable = self
            .peer_registry
            .protocol_capable_shard_peer_count(blockchain_id, STREAM_PROTOCOL_GET);
        observability::record_peer_registry_protocol_capable_shard_peers(
            blockchain_label,
            PROTOCOL_NAME_GET,
            get_capable,
        );
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct ProcessFdSnapshot {
    total: usize,
    sockets: usize,
    pipes: usize,
    anon_inodes: usize,
    files: usize,
    other: usize,
}

#[cfg(target_os = "linux")]
fn collect_process_fd_snapshot() -> Option<ProcessFdSnapshot> {
    let mut snapshot = ProcessFdSnapshot::default();
    let fd_entries = std::fs::read_dir("/proc/self/fd").ok()?;

    for entry in fd_entries.flatten() {
        snapshot.total = snapshot.total.saturating_add(1);
        match std::fs::read_link(entry.path()) {
            Ok(target) => classify_fd_target(&target, &mut snapshot),
            Err(_) => snapshot.other = snapshot.other.saturating_add(1),
        }
    }

    Some(snapshot)
}

#[cfg(target_os = "linux")]
fn classify_fd_target(target: &std::path::Path, snapshot: &mut ProcessFdSnapshot) {
    let target = target.to_string_lossy();
    if target.starts_with("socket:") {
        snapshot.sockets = snapshot.sockets.saturating_add(1);
    } else if target.starts_with("pipe:") {
        snapshot.pipes = snapshot.pipes.saturating_add(1);
    } else if target.starts_with("anon_inode:") {
        snapshot.anon_inodes = snapshot.anon_inodes.saturating_add(1);
    } else if target.starts_with('/') {
        snapshot.files = snapshot.files.saturating_add(1);
    } else {
        snapshot.other = snapshot.other.saturating_add(1);
    }
}

#[cfg(not(target_os = "linux"))]
fn collect_process_fd_snapshot() -> Option<ProcessFdSnapshot> {
    None
}
