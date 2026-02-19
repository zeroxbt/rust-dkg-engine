//! Sync periodic task implementation.
//!
//! Contains the `SyncTask` which orchestrates the three-stage sync pipeline.

use std::{sync::Arc, time::Duration};

use dkg_blockchain::{Address, BlockchainId, ContractName};
use futures::future::join_all;
use thiserror::Error;
use tokio::sync::mpsc;
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
    application::GET_NETWORK_CONCURRENT_PEERS, periodic_tasks::SyncDeps,
    periodic_tasks::runner::run_with_shutdown,
};

pub(crate) struct SyncTask {
    config: SyncConfig,
    deps: SyncDeps,
}

#[derive(Debug, Error)]
enum SyncTaskError {
    #[error("Failed to fetch pending KCs from queue")]
    FetchPendingKcs(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to fetch pending KCs after enqueue")]
    FetchPendingKcsAfterEnqueue(#[source] dkg_repository::error::RepositoryError),
    #[error("Filter task panicked")]
    FilterTaskPanicked(#[source] tokio::task::JoinError),
    #[error("Fetch task panicked")]
    FetchTaskPanicked(#[source] tokio::task::JoinError),
    #[error("Insert task panicked")]
    InsertTaskPanicked(#[source] tokio::task::JoinError),
    #[error("Failed to get latest KC ID from chain")]
    GetLatestKcId(#[source] dkg_blockchain::BlockchainError),
    #[error("Failed to load sync progress")]
    LoadSyncProgress(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to enqueue KCs")]
    EnqueueKcs(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to update sync progress")]
    UpdateSyncProgress(#[source] dkg_repository::error::RepositoryError),
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
    ) -> Result<ContractSyncResult, SyncTaskError> {
        let contract_addr_str = format!("{:?}", contract_address);
        let sync_start = std::time::Instant::now();

        // Step 1: Get pending KCs from queue

        let mut pending_kcs = self
            .deps
            .kc_sync_repository
            .get_pending_kcs_for_contract(
                blockchain_id.as_str(),
                &contract_addr_str,
                chrono::Utc::now().timestamp(),
                self.config.max_retry_attempts,
                self.config.max_new_kcs_per_contract,
            )
            .await
            .map_err(SyncTaskError::FetchPendingKcs)?;

        // Step 2: Enqueue new KCs if needed

        let existing_count = pending_kcs.len() as u64;
        let max_new_kcs_per_contract = self.config.max_new_kcs_per_contract;
        let enqueued = if existing_count < max_new_kcs_per_contract {
            let needed = max_new_kcs_per_contract - existing_count;
            let newly_enqueued = self
                .enqueue_new_kcs(blockchain_id, contract_address, &contract_addr_str, needed)
                .await?;

            if newly_enqueued > 0 {
                pending_kcs = self
                    .deps
                    .kc_sync_repository
                    .get_pending_kcs_for_contract(
                        blockchain_id.as_str(),
                        &contract_addr_str,
                        chrono::Utc::now().timestamp(),
                        self.config.max_retry_attempts,
                        max_new_kcs_per_contract,
                    )
                    .await
                    .map_err(SyncTaskError::FetchPendingKcsAfterEnqueue)?;
            }
            newly_enqueued
        } else {
            tracing::debug!(
                existing_count,
                "Queue already has enough pending KCs, skipping enqueue"
            );
            0
        };

        let pending = pending_kcs.len();

        if pending == 0 {
            tracing::debug!("No pending KCs");
            return Ok(ContractSyncResult {
                enqueued,
                pending: 0,
                synced: 0,
                failed: 0,
            });
        }

        let pending_kc_ids: Vec<u64> = pending_kcs.into_iter().map(|kc| kc.kc_id).collect();

        let (filter_stats, fetch_stats, insert_stats) = self
            .run_pipeline(
                pending_kc_ids,
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

        let total_ms = sync_start.elapsed().as_millis();

        tracing::info!(
            total_ms,
            kcs_pending = pending,
            kcs_already_synced = filter_stats.already_synced.len(),
            kcs_expired = filter_stats.expired.len(),
            kcs_fetch_failed = fetch_failures_count,
            kcs_synced = insert_stats.synced.len(),
            kcs_insert_failed = insert_failures_count,
            "Contract sync timing breakdown (pipelined)"
        );

        Ok(ContractSyncResult {
            enqueued,
            pending,
            synced,
            failed,
        })
    }

    /// Run the three-stage pipeline with spawned tasks.
    async fn run_pipeline(
        &self,
        pending_kc_ids: Vec<u64>,
        blockchain_id: BlockchainId,
        contract_address: Address,
        contract_addr_str: String,
    ) -> Result<(FilterStats, FetchStats, InsertStats), SyncTaskError> {
        let pipeline_channel_buffer = self.config.pipeline_channel_buffer.max(1);
        let filter_batch_size = self.config.filter_batch_size.max(1);
        let network_fetch_batch_size = self.config.network_fetch_batch_size.max(1);
        let max_assets_per_fetch_batch = self.config.max_assets_per_fetch_batch.max(1);

        // Create pipeline channels
        let (filter_tx, filter_rx) = mpsc::channel::<Vec<KcToSync>>(pipeline_channel_buffer);
        let (fetch_tx, fetch_rx) = mpsc::channel::<Vec<FetchedKc>>(pipeline_channel_buffer);

        // Spawn filter task (with current span as parent for trace propagation)
        let filter_handle = {
            let blockchain_id = blockchain_id.clone();
            let contract_addr_str = contract_addr_str.clone();
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
        };

        // Spawn fetch task (with current span as parent for trace propagation)
        let fetch_handle = {
            let blockchain_id = blockchain_id.clone();
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
        };

        // Spawn insert task (with current span as parent for trace propagation)
        let insert_handle = {
            let blockchain_id = blockchain_id.clone();
            let contract_addr_str = contract_addr_str.clone();
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
        };

        // Wait for all tasks
        let (filter_result, fetch_result, insert_result) =
            tokio::join!(filter_handle, fetch_handle, insert_handle);

        let filter_stats = filter_result.map_err(SyncTaskError::FilterTaskPanicked)?;
        let fetch_stats = fetch_result.map_err(SyncTaskError::FetchTaskPanicked)?;
        let insert_stats = insert_result.map_err(SyncTaskError::InsertTaskPanicked)?;

        Ok((filter_stats, fetch_stats, insert_stats))
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
        let repo = &self.deps.kc_sync_repository;

        // Remove already-synced KCs (found locally in filter stage)
        if !filter_stats.already_synced.is_empty()
            && let Err(e) = repo
                .remove_kcs(
                    blockchain_id.as_str(),
                    contract_addr_str,
                    &filter_stats.already_synced,
                )
                .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                "Failed to remove already-synced KCs from queue"
            );
        }

        // Remove expired KCs
        if !filter_stats.expired.is_empty()
            && let Err(e) = repo
                .remove_kcs(
                    blockchain_id.as_str(),
                    contract_addr_str,
                    &filter_stats.expired,
                )
                .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                "Failed to remove expired KCs from queue"
            );
        }

        // Remove successfully synced KCs
        if !insert_stats.synced.is_empty()
            && let Err(e) = repo
                .remove_kcs(
                    blockchain_id.as_str(),
                    contract_addr_str,
                    &insert_stats.synced,
                )
                .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                "Failed to remove synced KCs from queue"
            );
        }

        // Increment retry count for failed KCs
        let mut all_failed: Vec<u64> = fetch_stats.failures.to_vec();
        all_failed.extend(&insert_stats.failed);

        if !all_failed.is_empty()
            && let Err(e) = repo
                .increment_retry_count(
                    blockchain_id.as_str(),
                    contract_addr_str,
                    &all_failed,
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
    ) -> Result<u64, SyncTaskError> {
        let latest_on_chain = self
            .deps
            .blockchain_manager
            .get_latest_knowledge_collection_id(blockchain_id, contract_address)
            .await
            .map_err(SyncTaskError::GetLatestKcId)?;

        let last_checked = self
            .deps
            .kc_sync_repository
            .get_progress(blockchain_id.as_str(), contract_addr_str)
            .await
            .map_err(SyncTaskError::LoadSyncProgress)?
            .map(|p| p.last_checked_id)
            .unwrap_or(0);

        if latest_on_chain <= last_checked {
            tracing::trace!(latest_on_chain, last_checked, "No new KCs to enqueue");
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
            .map_err(SyncTaskError::EnqueueKcs)?;

        self.deps
            .kc_sync_repository
            .upsert_progress(blockchain_id.as_str(), contract_addr_str, end_id)
            .await
            .map_err(SyncTaskError::UpdateSyncProgress)?;

        tracing::debug!(
            latest_on_chain,
            last_checked,
            start_id,
            end_id,
            enqueued = count,
            "Enqueued new KCs for sync"
        );

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
        let idle_period = Duration::from_secs(self.config.period_idle_secs);
        let catching_up_period = Duration::from_secs(self.config.period_catching_up_secs);
        let no_peers_retry_delay = Duration::from_secs(self.config.no_peers_retry_delay_secs);

        if !self.config.enabled {
            tracing::trace!("Sync task disabled by configuration");
            return idle_period;
        }

        // Check if we have identified enough shard peers before attempting sync
        let total_shard_peers = self.deps.peer_registry.shard_peer_count(blockchain_id);
        let identified_peers = self
            .deps
            .peer_registry
            .identified_shard_peer_count(blockchain_id);
        let min_required = (total_shard_peers / 3).max(GET_NETWORK_CONCURRENT_PEERS);

        if identified_peers < min_required {
            tracing::debug!(
                identified = identified_peers,
                total = total_shard_peers,
                required = min_required,
                "Not enough shard peers identified yet, retrying later"
            );
            return no_peers_retry_delay;
        }

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
                return idle_period;
            }
        };

        // Sync each contract in parallel
        let sync_futures = contract_addresses
            .iter()
            .map(|&contract_address| self.sync_contract(blockchain_id, contract_address));

        let results = join_all(sync_futures).await;

        // Aggregate results
        let mut total_enqueued = 0u64;
        let mut total_pending = 0usize;
        let mut total_synced = 0u64;
        let mut total_failed = 0u64;

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(r) => {
                    total_enqueued += r.enqueued;
                    total_pending += r.pending;
                    total_synced += r.synced;
                    total_failed += r.failed;

                    if r.enqueued > 0 || r.pending > 0 {
                        tracing::trace!(
                            contract = ?contract_addresses[i],
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
                        contract = ?contract_addresses[i],
                        error = %e,
                        "Failed to sync contract"
                    );
                }
            }
        }

        if total_enqueued > 0 || total_pending > 0 {
            tracing::info!(
                contract_count = contract_addresses.len(),
                identified_peers,
                total_shard_peers,
                total_enqueued,
                total_pending,
                total_synced,
                total_failed,
                "Sync cycle summary"
            );
        }

        // Use short delay while catching up, longer delay when idle
        // - total_pending > 0: still have KCs in queue to process
        // - total_enqueued > 0: just discovered new KCs on chain (might be more due to limit)

        if total_pending > 0 || total_enqueued > 0 {
            tracing::trace!(
                total_pending,
                total_enqueued,
                delay_secs = catching_up_period.as_secs(),
                "Scheduling catch-up resync"
            );
            catching_up_period
        } else {
            tracing::trace!(
                delay_secs = idle_period.as_secs(),
                "Scheduling idle sync poll"
            );
            idle_period
        }
    }
}
