//! Sync periodic task implementation.
//!
//! Contains the `SyncTask` which orchestrates the three-stage sync pipeline.

use std::{sync::Arc, time::Duration};

use dkg_blockchain::{Address, BlockchainId, ContractName};
use futures::future::join_all;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::{
    SyncConfig,
    fetch::fetch_task,
    filter::filter_task,
    insert::insert_task,
    types::{ContractSyncResult, FetchStats, FetchedKc, FilterStats, InsertStats, KcToSync},
};
use crate::{
    application::NETWORK_CONCURRENT_PEERS, periodic_tasks::SyncDeps,
    periodic_tasks::runner::run_with_shutdown,
};

pub(crate) struct SyncTask {
    config: SyncConfig,
    deps: SyncDeps,
}

struct PendingContractBatch {
    enqueued: u64,
    pending_kc_ids: Vec<u64>,
}

#[derive(Default)]
struct SyncCycleTotals {
    total_enqueued: u64,
    total_pending: usize,
    total_synced: u64,
    total_failed: u64,
}

struct SyncCycleDurations {
    idle: Duration,
    catching_up: Duration,
    no_peers_retry: Duration,
}

fn min_required_identified_peers(total_shard_peers: usize) -> usize {
    (total_shard_peers / 3).max(NETWORK_CONCURRENT_PEERS)
}

fn should_use_catching_up_delay(totals: &SyncCycleTotals) -> bool {
    totals.total_pending > 0 || totals.total_enqueued > 0
}

impl SyncTask {
    pub(crate) fn new(deps: SyncDeps, config: SyncConfig) -> Self {
        Self { config, deps }
    }

    /// Sync a single contract using a three-stage pipeline.
    ///
    /// Pipeline stages connected by channels:
    /// 1. Filter: checks RPC for token ranges, triple store for local existence
    /// 2. Fetch: fetches KCs from network peers
    /// 3. Insert: checks expiration, inserts into triple store
    #[tracing::instrument(
        skip(self),
        fields(
            blockchain_id = %blockchain_id,
            contract = %format!("{:?}", contract_address),
        )
    )]
    async fn sync_contract(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
    ) -> Result<ContractSyncResult, String> {
        let contract_addr_str = format!("{:?}", contract_address);
        let sync_start = std::time::Instant::now();

        let pending_batch = self
            .collect_pending_contract_batch(blockchain_id, contract_address, &contract_addr_str)
            .await?;
        let enqueued = pending_batch.enqueued;
        let pending = pending_batch.pending_kc_ids.len();

        if pending == 0 {
            tracing::debug!("No pending KCs");
            return Ok(ContractSyncResult {
                enqueued,
                pending: 0,
                synced: 0,
                failed: 0,
            });
        }

        let (filter_stats, fetch_stats, insert_stats) = self
            .run_pipeline(
                pending_batch.pending_kc_ids,
                blockchain_id.clone(),
                contract_address,
                contract_addr_str.clone(),
            )
            .await?;

        // Step 4: Update DB with results
        self.update_sync_queue(
            blockchain_id,
            &contract_addr_str,
            &filter_stats,
            &fetch_stats,
            &insert_stats,
        )
        .await;

        // Calculate totals
        let fetch_failures_count = fetch_stats.failures.len();
        let insert_failures_count = insert_stats.failed.len();
        let synced = filter_stats.already_synced.len() as u64 + insert_stats.synced.len() as u64;
        let failed = (fetch_failures_count + insert_failures_count) as u64;
        self.log_contract_sync_timing(
            sync_start,
            pending,
            &filter_stats,
            fetch_failures_count,
            &insert_stats,
        );

        Ok(ContractSyncResult {
            enqueued,
            pending,
            synced,
            failed,
        })
    }

    async fn collect_pending_contract_batch(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        contract_addr_str: &str,
    ) -> Result<PendingContractBatch, String> {
        let mut pending_kcs = self
            .deps
            .kc_sync_repository
            .get_pending_kcs_for_contract(
                blockchain_id.as_str(),
                contract_addr_str,
                chrono::Utc::now().timestamp(),
                self.config.max_retry_attempts,
                self.config.max_new_kcs_per_contract,
            )
            .await
            .map_err(|e| format!("Failed to fetch pending KCs: {}", e))?;

        let existing_count = pending_kcs.len() as u64;
        let max_new_kcs_per_contract = self.config.max_new_kcs_per_contract;
        let enqueued = if existing_count < max_new_kcs_per_contract {
            let needed = max_new_kcs_per_contract - existing_count;
            let newly_enqueued = self
                .enqueue_new_kcs(blockchain_id, contract_address, contract_addr_str, needed)
                .await?;

            if newly_enqueued > 0 {
                pending_kcs = self
                    .deps
                    .kc_sync_repository
                    .get_pending_kcs_for_contract(
                        blockchain_id.as_str(),
                        contract_addr_str,
                        chrono::Utc::now().timestamp(),
                        self.config.max_retry_attempts,
                        max_new_kcs_per_contract,
                    )
                    .await
                    .map_err(|e| format!("Failed to fetch pending KCs after enqueue: {}", e))?;
            }
            newly_enqueued
        } else {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                existing_count,
                "Queue already has enough pending KCs, skipping enqueue"
            );
            0
        };

        Ok(PendingContractBatch {
            enqueued,
            pending_kc_ids: pending_kcs.into_iter().map(|kc| kc.kc_id).collect(),
        })
    }

    fn log_contract_sync_timing(
        &self,
        sync_start: std::time::Instant,
        pending: usize,
        filter_stats: &FilterStats,
        fetch_failures_count: usize,
        insert_stats: &InsertStats,
    ) {
        let total_ms = sync_start.elapsed().as_millis();
        tracing::info!(
            total_ms,
            kcs_pending = pending,
            kcs_already_synced = filter_stats.already_synced.len(),
            kcs_expired = filter_stats.expired.len(),
            kcs_fetch_failed = fetch_failures_count,
            kcs_synced = insert_stats.synced.len(),
            kcs_insert_failed = insert_stats.failed.len(),
            "Contract sync timing breakdown (pipelined)"
        );
    }

    /// Run the three-stage pipeline with spawned tasks.
    async fn run_pipeline(
        &self,
        pending_kc_ids: Vec<u64>,
        blockchain_id: BlockchainId,
        contract_address: Address,
        contract_addr_str: String,
    ) -> Result<(FilterStats, FetchStats, InsertStats), String> {
        let pipeline_channel_buffer = self.config.pipeline_channel_buffer.max(1);
        let filter_batch_size = self.config.filter_batch_size.max(1);
        let network_fetch_batch_size = self.config.network_fetch_batch_size.max(1);
        let max_assets_per_fetch_batch = self.config.max_assets_per_fetch_batch.max(1);

        // Create pipeline channels
        let (filter_tx, filter_rx) = mpsc::channel::<Vec<KcToSync>>(pipeline_channel_buffer);
        let (fetch_tx, fetch_rx) = mpsc::channel::<Vec<FetchedKc>>(pipeline_channel_buffer);

        let filter_handle = self.spawn_filter_stage(
            pending_kc_ids,
            filter_batch_size,
            blockchain_id.clone(),
            contract_address,
            contract_addr_str.clone(),
            filter_tx,
        );
        let fetch_handle = self.spawn_fetch_stage(
            filter_rx,
            network_fetch_batch_size,
            max_assets_per_fetch_batch,
            blockchain_id.clone(),
            fetch_tx,
        );
        let insert_handle = self.spawn_insert_stage(fetch_rx, blockchain_id, contract_addr_str);

        // Wait for all tasks
        let (filter_result, fetch_result, insert_result) =
            tokio::join!(filter_handle, fetch_handle, insert_handle);

        let filter_stats = filter_result.map_err(|e| format!("Filter task panicked: {}", e))?;
        let fetch_stats = fetch_result.map_err(|e| format!("Fetch task panicked: {}", e))?;
        let insert_stats = insert_result.map_err(|e| format!("Insert task panicked: {}", e))?;

        Ok((filter_stats, fetch_stats, insert_stats))
    }

    fn spawn_filter_stage(
        &self,
        pending_kc_ids: Vec<u64>,
        filter_batch_size: usize,
        blockchain_id: BlockchainId,
        contract_address: Address,
        contract_addr_str: String,
        filter_tx: mpsc::Sender<Vec<KcToSync>>,
    ) -> JoinHandle<FilterStats> {
        let triple_store_assertions = Arc::clone(&self.deps.triple_store_assertions);
        let blockchain_manager = Arc::clone(&self.deps.blockchain_manager);
        tokio::spawn(
            async move {
                filter_task(
                    pending_kc_ids,
                    filter_batch_size,
                    blockchain_id,
                    contract_address,
                    contract_addr_str,
                    blockchain_manager,
                    triple_store_assertions,
                    filter_tx,
                )
                .await
            }
            .in_current_span(),
        )
    }

    fn spawn_fetch_stage(
        &self,
        filter_rx: mpsc::Receiver<Vec<KcToSync>>,
        network_fetch_batch_size: usize,
        max_assets_per_fetch_batch: u64,
        blockchain_id: BlockchainId,
        fetch_tx: mpsc::Sender<Vec<FetchedKc>>,
    ) -> JoinHandle<FetchStats> {
        let network_manager = Arc::clone(&self.deps.network_manager);
        let assertion_validation = Arc::clone(&self.deps.assertion_validation);
        let peer_registry = Arc::clone(&self.deps.peer_registry);
        tokio::spawn(
            async move {
                fetch_task(
                    filter_rx,
                    network_fetch_batch_size,
                    max_assets_per_fetch_batch,
                    blockchain_id,
                    network_manager,
                    assertion_validation,
                    peer_registry,
                    fetch_tx,
                )
                .await
            }
            .in_current_span(),
        )
    }

    fn spawn_insert_stage(
        &self,
        fetch_rx: mpsc::Receiver<Vec<FetchedKc>>,
        blockchain_id: BlockchainId,
        contract_addr_str: String,
    ) -> JoinHandle<InsertStats> {
        let triple_store_assertions = Arc::clone(&self.deps.triple_store_assertions);
        tokio::spawn(
            async move {
                insert_task(
                    fetch_rx,
                    blockchain_id,
                    contract_addr_str,
                    triple_store_assertions,
                )
                .await
            }
            .in_current_span(),
        )
    }

    /// Update the sync queue based on pipeline results.
    async fn update_sync_queue(
        &self,
        blockchain_id: &BlockchainId,
        contract_addr_str: &str,
        filter_stats: &FilterStats,
        fetch_stats: &FetchStats,
        insert_stats: &InsertStats,
    ) {
        self.remove_kcs_from_queue(
            blockchain_id,
            contract_addr_str,
            &filter_stats.already_synced,
            "already-synced",
        )
        .await;
        self.remove_kcs_from_queue(
            blockchain_id,
            contract_addr_str,
            &filter_stats.expired,
            "expired",
        )
        .await;
        self.remove_kcs_from_queue(
            blockchain_id,
            contract_addr_str,
            &insert_stats.synced,
            "synced",
        )
        .await;

        // Increment retry count for failed KCs
        let mut all_failed: Vec<u64> = fetch_stats.failures.to_vec();
        all_failed.extend(&insert_stats.failed);

        self.increment_retry_count_for_failures(blockchain_id, contract_addr_str, &all_failed)
            .await;
    }

    async fn remove_kcs_from_queue(
        &self,
        blockchain_id: &BlockchainId,
        contract_addr_str: &str,
        kc_ids: &[u64],
        reason: &str,
    ) {
        if kc_ids.is_empty() {
            return;
        }

        if let Err(e) = self
            .deps
            .kc_sync_repository
            .remove_kcs(blockchain_id.as_str(), contract_addr_str, kc_ids)
            .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                reason = %reason,
                error = %e,
                "Failed to remove KCs from sync queue"
            );
        }
    }

    async fn increment_retry_count_for_failures(
        &self,
        blockchain_id: &BlockchainId,
        contract_addr_str: &str,
        failed_kc_ids: &[u64],
    ) {
        if failed_kc_ids.is_empty() {
            return;
        }

        if let Err(e) = self
            .deps
            .kc_sync_repository
            .increment_retry_count(
                blockchain_id.as_str(),
                contract_addr_str,
                failed_kc_ids,
                self.config.retry_base_delay_secs,
                self.config.retry_max_delay_secs,
                self.config.retry_jitter_secs,
            )
            .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                "Failed to increment retry count for failed KCs"
            );
        }
    }

    /// Check for new KCs on chain and enqueue any that need syncing.
    async fn enqueue_new_kcs(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        contract_addr_str: &str,
        limit: u64,
    ) -> Result<u64, String> {
        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            "Fetching latest KC ID from chain..."
        );

        let latest_on_chain = self
            .deps
            .blockchain_manager
            .get_latest_knowledge_collection_id(blockchain_id, contract_address)
            .await
            .map_err(|e| format!("Failed to get latest KC ID: {}", e))?;

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            latest_on_chain,
            "Got latest KC ID from chain"
        );

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            "Fetching sync progress from DB..."
        );

        let last_checked = self
            .deps
            .kc_sync_repository
            .get_progress(blockchain_id.as_str(), contract_addr_str)
            .await
            .map_err(|e| format!("Failed to get sync progress: {}", e))?
            .map(|p| p.last_checked_id)
            .unwrap_or(0);

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            last_checked,
            "Got sync progress from DB"
        );

        if latest_on_chain <= last_checked {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                "No new KCs to enqueue (latest_on_chain <= last_checked)"
            );
            return Ok(0);
        }

        let start_id = last_checked + 1;
        let end_id = std::cmp::min(latest_on_chain, last_checked + limit);
        let new_kc_ids: Vec<u64> = (start_id..=end_id).collect();
        let count = new_kc_ids.len() as u64;

        self.deps
            .kc_sync_repository
            .enqueue_kcs(blockchain_id.as_str(), contract_addr_str, &new_kc_ids)
            .await
            .map_err(|e| format!("Failed to enqueue KCs: {}", e))?;

        self.deps
            .kc_sync_repository
            .upsert_progress(blockchain_id.as_str(), contract_addr_str, end_id)
            .await
            .map_err(|e| format!("Failed to update progress: {}", e))?;

        Ok(count)
    }
}

impl SyncTask {
    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("sync", shutdown, || self.execute(blockchain_id)).await;
    }

    #[tracing::instrument(
        name = "periodic_tasks.sync",
        skip(self),
        fields(blockchain_id = %blockchain_id)
    )]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let periods = self.cycle_durations();

        if !self.config.enabled {
            tracing::trace!(
                blockchain_id = %blockchain_id,
                "Sync task disabled by configuration"
            );
            return periods.idle;
        }

        if let Some(delay) = self.delay_for_unready_peer_state(blockchain_id, &periods) {
            return delay;
        }

        let Some(contract_addresses) = self.load_contract_addresses(blockchain_id).await else {
            return periods.idle;
        };

        let totals = self
            .run_contract_sync_cycle(blockchain_id, &contract_addresses)
            .await;

        self.next_cycle_delay(blockchain_id, &totals, &periods)
    }

    fn cycle_durations(&self) -> SyncCycleDurations {
        SyncCycleDurations {
            idle: Duration::from_secs(self.config.period_idle_secs),
            catching_up: Duration::from_secs(self.config.period_catching_up_secs),
            no_peers_retry: Duration::from_secs(self.config.no_peers_retry_delay_secs),
        }
    }

    fn delay_for_unready_peer_state(
        &self,
        blockchain_id: &BlockchainId,
        periods: &SyncCycleDurations,
    ) -> Option<Duration> {
        let total_shard_peers = self.deps.peer_registry.shard_peer_count(blockchain_id);
        let identified_peers = self
            .deps
            .peer_registry
            .identified_shard_peer_count(blockchain_id);
        let min_required = min_required_identified_peers(total_shard_peers);

        if identified_peers < min_required {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                identified = identified_peers,
                total = total_shard_peers,
                required = min_required,
                "Not enough shard peers identified yet, retrying later"
            );
            return Some(periods.no_peers_retry);
        }

        tracing::debug!(
            blockchain_id = %blockchain_id,
            identified_peers,
            total_shard_peers,
            "Starting sync cycle"
        );
        None
    }

    async fn load_contract_addresses(&self, blockchain_id: &BlockchainId) -> Option<Vec<Address>> {
        let contract_addresses = match self
            .deps
            .blockchain_manager
            .get_all_contract_addresses(blockchain_id, &ContractName::KnowledgeCollectionStorage)
            .await
        {
            Ok(addresses) => addresses,
            Err(e) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    error = %e,
                    "Failed to get KC storage contract addresses"
                );
                return None;
            }
        };

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract_count = contract_addresses.len(),
            "Found KC storage contracts"
        );
        Some(contract_addresses)
    }

    async fn run_contract_sync_cycle(
        &self,
        blockchain_id: &BlockchainId,
        contract_addresses: &[Address],
    ) -> SyncCycleTotals {
        let sync_futures = contract_addresses
            .iter()
            .map(|&contract_address| self.sync_contract(blockchain_id, contract_address));
        let results = join_all(sync_futures).await;

        let mut totals = SyncCycleTotals::default();
        for (i, result) in results.into_iter().enumerate() {
            self.aggregate_contract_result(
                blockchain_id,
                contract_addresses[i],
                result,
                &mut totals,
            );
        }
        totals
    }

    fn aggregate_contract_result(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        result: Result<ContractSyncResult, String>,
        totals: &mut SyncCycleTotals,
    ) {
        match result {
            Ok(r) => {
                totals.total_enqueued += r.enqueued;
                totals.total_pending += r.pending;
                totals.total_synced += r.synced;
                totals.total_failed += r.failed;

                if r.enqueued > 0 || r.pending > 0 {
                    tracing::debug!(
                        blockchain_id = %blockchain_id,
                        contract = ?contract_address,
                        enqueued = r.enqueued,
                        pending = r.pending,
                        synced = r.synced,
                        failed = r.failed,
                        "Contract sync completed"
                    );
                }
            }
            Err(e) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = ?contract_address,
                    error = %e,
                    "Failed to sync contract"
                );
            }
        }
    }

    fn next_cycle_delay(
        &self,
        blockchain_id: &BlockchainId,
        totals: &SyncCycleTotals,
        periods: &SyncCycleDurations,
    ) -> Duration {
        if totals.total_enqueued > 0 || totals.total_pending > 0 {
            tracing::info!(
                blockchain_id = %blockchain_id,
                total_enqueued = totals.total_enqueued,
                total_pending = totals.total_pending,
                total_synced = totals.total_synced,
                total_failed = totals.total_failed,
                "Sync cycle summary"
            );
        }

        if should_use_catching_up_delay(totals) {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                total_pending = totals.total_pending,
                total_enqueued = totals.total_enqueued,
                "Still catching up, scheduling immediate resync"
            );
            periods.catching_up
        } else {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                "Caught up, scheduling idle poll"
            );
            periods.idle
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_required_identified_peers_uses_network_floor() {
        assert_eq!(min_required_identified_peers(0), NETWORK_CONCURRENT_PEERS);
        assert_eq!(min_required_identified_peers(2), NETWORK_CONCURRENT_PEERS);
    }

    #[test]
    fn min_required_identified_peers_grows_with_shard_size() {
        let total_shard_peers = NETWORK_CONCURRENT_PEERS * 9;
        assert_eq!(
            min_required_identified_peers(total_shard_peers),
            total_shard_peers / 3
        );
    }

    #[test]
    fn catching_up_delay_selected_when_pending_or_enqueued() {
        let totals = SyncCycleTotals {
            total_enqueued: 1,
            total_pending: 0,
            total_synced: 0,
            total_failed: 0,
        };
        assert!(should_use_catching_up_delay(&totals));

        let totals = SyncCycleTotals {
            total_enqueued: 0,
            total_pending: 3,
            total_synced: 0,
            total_failed: 0,
        };
        assert!(should_use_catching_up_delay(&totals));
    }

    #[test]
    fn idle_delay_selected_when_no_backlog_and_no_new_items() {
        let totals = SyncCycleTotals::default();
        assert!(!should_use_catching_up_delay(&totals));
    }
}
