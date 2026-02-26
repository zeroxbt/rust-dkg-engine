//! Data sync task orchestration (queue consumer).

use std::{sync::Arc, time::Duration};

use dkg_blockchain::{
    Address, BlockchainId, ContractName, MulticallBatch, MulticallRequest, encoders,
};
use dkg_observability as observability;
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
    application::state_metadata::encode_burned_ids, periodic_tasks::SyncDeps,
    periodic_tasks::runner::run_with_shutdown,
};

pub(crate) struct SyncTask {
    config: SyncConfig,
    deps: SyncDeps,
}

#[derive(Debug, Error)]
enum SyncTaskError {
    #[error("Failed to fetch due KC IDs from queue")]
    FetchDueKcIds(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to query missing sync state")]
    GetMissingKcStateMetadataIds(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to execute state hydration multicall")]
    HydrationMulticall(#[source] dkg_blockchain::BlockchainError),
    #[error("Failed to upsert sync state")]
    UpsertKcStateMetadata(#[source] dkg_repository::error::RepositoryError),
    #[error("Filter task panicked")]
    FilterTaskPanicked(#[source] tokio::task::JoinError),
    #[error("Fetch task panicked")]
    FetchTaskPanicked(#[source] tokio::task::JoinError),
    #[error("Insert task panicked")]
    InsertTaskPanicked(#[source] tokio::task::JoinError),
}

impl SyncTask {
    pub(crate) fn new(deps: SyncDeps, config: SyncConfig) -> Self {
        Self { config, deps }
    }

    #[tracing::instrument(
        skip(self),
        fields(blockchain_id = %blockchain_id, contract = %format!("{:?}", contract_address))
    )]
    async fn sync_contract(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        target_tip: u64,
        allow_pipeline_fetch: bool,
    ) -> Result<ContractSyncResult, SyncTaskError> {
        let contract_addr_str = format!("{:?}", contract_address);

        let now_ts = chrono::Utc::now().timestamp();
        let pending_ids = self
            .deps
            .kc_sync_repository
            .get_due_kc_ids_for_contract(
                blockchain_id.as_str(),
                &contract_addr_str,
                now_ts,
                self.config.max_retry_attempts,
                self.config.max_new_kcs_per_contract.max(1),
            )
            .await
            .map_err(SyncTaskError::FetchDueKcIds)?;
        let pending = pending_ids.len();

        if pending == 0 {
            return Ok(ContractSyncResult {
                kc_state_metadata_hydrated: 0,
                pending: 0,
                synced: 0,
                failed: 0,
            });
        }

        let kc_state_metadata_hydrated = self
            .hydrate_kc_state_metadata_for_contract(
                blockchain_id,
                contract_address,
                &contract_addr_str,
                &pending_ids,
                target_tip,
            )
            .await?;

        if !allow_pipeline_fetch {
            return Ok(ContractSyncResult {
                kc_state_metadata_hydrated,
                pending,
                synced: 0,
                failed: 0,
            });
        }

        let (filter_stats, fetch_stats, insert_stats) = self
            .run_pipeline(
                pending_ids,
                blockchain_id.clone(),
                contract_address,
                contract_addr_str.clone(),
            )
            .await?;

        self.update_sync_queue(
            blockchain_id,
            &contract_addr_str,
            &filter_stats,
            &fetch_stats,
            &insert_stats,
        )
        .await;

        let fetch_failures_count = fetch_stats.failures.len();
        let insert_failures_count = insert_stats.failed.len();
        let synced = filter_stats.already_synced.len() as u64 + insert_stats.synced.len() as u64;
        let failed = (fetch_failures_count + insert_failures_count) as u64;

        tracing::info!(
            kcs_pending = pending,
            kcs_kc_state_metadata_hydrated = kc_state_metadata_hydrated,
            kcs_already_synced = filter_stats.already_synced.len(),
            kcs_expired = filter_stats.expired.len(),
            kcs_waiting_for_metadata = filter_stats.waiting_for_metadata.len(),
            kcs_waiting_for_state = filter_stats.waiting_for_state.len(),
            kcs_fetch_failed = fetch_failures_count,
            kcs_synced = insert_stats.synced.len(),
            kcs_insert_failed = insert_failures_count,
            "Data sync contract summary"
        );

        Ok(ContractSyncResult {
            kc_state_metadata_hydrated,
            pending,
            synced,
            failed,
        })
    }

    async fn hydrate_kc_state_metadata_for_contract(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        contract_addr_str: &str,
        pending_ids: &[u64],
        target_tip: u64,
    ) -> Result<u64, SyncTaskError> {
        if pending_ids.is_empty() {
            return Ok(0);
        }

        let candidate_ids: Vec<u64> = pending_ids
            .iter()
            .copied()
            .take(self.config.metadata_state_batch_size.max(1))
            .collect();

        let missing = self
            .deps
            .kc_chain_metadata_repository
            .get_ids_missing_kc_state_metadata(blockchain_id.as_str(), contract_addr_str, &candidate_ids)
            .await
            .map_err(SyncTaskError::GetMissingKcStateMetadataIds)?;
        if missing.is_empty() {
            return Ok(0);
        }

        let mut missing_ids: Vec<u64> = missing.into_iter().collect();
        missing_ids.sort_unstable();
        let mut updated = 0_u64;
        let chunk_size = self.config.metadata_stage_batch_size.max(1);

        for chunk in missing_ids.chunks(chunk_size) {
            let batch_started = std::time::Instant::now();
            let mut batch = MulticallBatch::with_capacity(chunk.len() * 3);
            for &kc_id in chunk {
                let kc = kc_id as u128;
                batch.add(MulticallRequest::new(
                    contract_address,
                    encoders::encode_get_end_epoch(kc),
                ));
                batch.add(MulticallRequest::new(
                    contract_address,
                    encoders::encode_get_knowledge_assets_range(kc),
                ));
                batch.add(MulticallRequest::new(
                    contract_address,
                    encoders::encode_get_merkle_root(kc),
                ));
            }

            let results = match self
                .deps
                .blockchain_manager
                .execute_multicall(blockchain_id, batch)
                .await
            {
                Ok(results) => results,
                Err(error) => {
                    observability::record_kc_state_metadata_hydration_batch(
                        blockchain_id.as_str(),
                        "error",
                        batch_started.elapsed(),
                        chunk.len(),
                    );
                    return Err(SyncTaskError::HydrationMulticall(error));
                }
            };

            for (kc_id, call_results) in chunk.iter().zip(results.chunks(3)) {
                let [epoch_result, range_result, merkle_result] = call_results else {
                    continue;
                };
                let Some((start, end, burned)) = range_result.as_knowledge_assets_range() else {
                    continue;
                };
                let Some(latest_merkle_root) = merkle_result.as_bytes32_hex() else {
                    continue;
                };
                let end_epoch = epoch_result.as_u64().filter(|v| *v != 0);

                let encoded = encode_burned_ids(start, end, &burned);
                self.deps
                    .kc_chain_metadata_repository
                    .upsert_kc_state_metadata(
                        blockchain_id.as_str(),
                        contract_addr_str,
                        *kc_id,
                        start,
                        end,
                        encoded.mode as u32,
                        encoded.payload.as_slice(),
                        end_epoch,
                        &latest_merkle_root,
                        target_tip,
                        Some("kc_state_metadata_hydration"),
                    )
                    .await
                    .map_err(SyncTaskError::UpsertKcStateMetadata)?;
                updated = updated.saturating_add(1);
            }

            observability::record_kc_state_metadata_hydration_batch(
                blockchain_id.as_str(),
                "ok",
                batch_started.elapsed(),
                chunk.len(),
            );
        }

        observability::record_kc_state_metadata_observed_lag(blockchain_id.as_str(), contract_addr_str, 0);
        Ok(updated)
    }

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
        let batch_get_fanout_concurrency = self.config.batch_get_fanout_concurrency.max(1);
        let max_assets_per_fetch_batch = self.config.max_assets_per_fetch_batch.max(1);
        let insert_batch_concurrency = self.config.insert_batch_concurrency.max(1);

        let (filter_tx, filter_rx) = mpsc::channel::<Vec<KcToSync>>(pipeline_channel_buffer);
        let (fetch_tx, fetch_rx) = mpsc::channel::<Vec<FetchedKc>>(pipeline_channel_buffer);

        let filter_handle = {
            let blockchain_id = blockchain_id.clone();
            let contract_addr_str = contract_addr_str.clone();
            let kc_chain_metadata_repository = self.deps.kc_chain_metadata_repository.clone();
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
                        kc_chain_metadata_repository,
                        triple_store_assertions,
                        filter_tx,
                    )
                    .await
                }
                .in_current_span(),
            )
        };

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
                        batch_get_fanout_concurrency,
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
                        insert_batch_concurrency,
                        triple_store_assertions,
                    )
                    .await
                }
                .in_current_span(),
            )
        };

        let (filter_result, fetch_result, insert_result) =
            tokio::join!(filter_handle, fetch_handle, insert_handle);

        let filter_stats = filter_result.map_err(SyncTaskError::FilterTaskPanicked)?;
        let fetch_stats = fetch_result.map_err(SyncTaskError::FetchTaskPanicked)?;
        let insert_stats = insert_result.map_err(SyncTaskError::InsertTaskPanicked)?;

        Ok((filter_stats, fetch_stats, insert_stats))
    }

    async fn update_sync_queue(
        &self,
        blockchain_id: &BlockchainId,
        contract_addr_str: &str,
        filter_stats: &FilterStats,
        fetch_stats: &FetchStats,
        insert_stats: &InsertStats,
    ) {
        let repo = &self.deps.kc_sync_repository;

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
}

impl SyncTask {
    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("sync_data", shutdown, || self.execute(blockchain_id)).await;
    }

    #[tracing::instrument(name = "periodic_tasks.sync_data", skip(self), fields(blockchain_id = %blockchain_id))]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let cycle_started = std::time::Instant::now();
        let blockchain_label = blockchain_id.as_str();
        let rss_start = process_rss_bytes();
        if let Some(rss) = rss_start {
            observability::record_sync_cycle_rss_bytes(blockchain_label, "start", rss);
        }

        let no_peers_retry_delay = Duration::from_secs(self.config.no_peers_retry_delay_secs);
        let idle_period = Duration::from_secs(self.config.sync_idle_sleep_secs.max(1));

        if !self.config.enabled {
            finalize_sync_cycle_metrics(
                blockchain_label,
                "disabled",
                cycle_started,
                rss_start,
                0,
                0,
                0,
                0,
                0,
            );
            return idle_period;
        }

        let current_block = match self
            .deps
            .blockchain_manager
            .get_block_number(blockchain_id)
            .await
        {
            Ok(block) => block,
            Err(error) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to resolve current block for data sync cycle"
                );
                finalize_sync_cycle_metrics(
                    blockchain_label,
                    "block_error",
                    cycle_started,
                    rss_start,
                    0,
                    0,
                    0,
                    0,
                    0,
                );
                return idle_period;
            }
        };
        let target_tip = current_block.saturating_sub(self.config.head_safety_blocks);

        let total_shard_peers = self.deps.peer_registry.shard_peer_count(blockchain_id);
        let identified_peers = self
            .deps
            .peer_registry
            .identified_shard_peer_count(blockchain_id);
        let min_required = (total_shard_peers / 3).max(3);
        let enough_peers = identified_peers >= min_required;

        let contract_addresses = match self
            .deps
            .blockchain_manager
            .get_all_contract_addresses(blockchain_id, &ContractName::KnowledgeCollectionStorage)
            .await
        {
            Ok(addresses) => addresses,
            Err(error) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to get KC storage contract addresses"
                );
                finalize_sync_cycle_metrics(
                    blockchain_label,
                    "contracts_error",
                    cycle_started,
                    rss_start,
                    0,
                    0,
                    0,
                    0,
                    0,
                );
                return idle_period;
            }
        };

        let sync_futures = contract_addresses.iter().map(|&contract_address| {
            self.sync_contract(blockchain_id, contract_address, target_tip, enough_peers)
        });
        let results = futures::future::join_all(sync_futures).await;

        let mut total_kc_state_metadata_hydrated = 0_u64;
        let mut total_pending = 0_usize;
        let mut total_synced = 0_u64;
        let mut total_failed = 0_u64;

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(r) => {
                    total_kc_state_metadata_hydrated += r.kc_state_metadata_hydrated;
                    total_pending += r.pending;
                    total_synced += r.synced;
                    total_failed += r.failed;

                    if r.pending > 0 {
                        tracing::trace!(
                            contract = ?contract_addresses[i],
                            kc_state_metadata_hydrated = r.kc_state_metadata_hydrated,
                            pending = r.pending,
                            synced = r.synced,
                            failed = r.failed,
                            "Data sync contract completed"
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(
                        blockchain_id = %blockchain_id,
                        contract = ?contract_addresses[i],
                        error = %error,
                        "Failed to sync contract data"
                    );
                }
            }
        }

        let status = if enough_peers {
            "ok"
        } else {
            "waiting_for_peers"
        };
        finalize_sync_cycle_metrics(
            blockchain_label,
            status,
            cycle_started,
            rss_start,
            contract_addresses.len(),
            0,
            total_pending,
            total_synced,
            total_failed,
        );
        observability::record_sync_last_success_heartbeat();

        if !enough_peers {
            return no_peers_retry_delay;
        }

        if total_pending > 0 || total_kc_state_metadata_hydrated > 0 {
            return Duration::ZERO;
        }

        idle_period
    }
}

#[allow(clippy::too_many_arguments)]
fn finalize_sync_cycle_metrics(
    blockchain_id: &str,
    status: &str,
    started: std::time::Instant,
    rss_start: Option<u64>,
    contracts: usize,
    enqueued: u64,
    pending: usize,
    synced: u64,
    failed: u64,
) {
    observability::record_sync_cycle(
        blockchain_id,
        status,
        started.elapsed(),
        contracts,
        enqueued,
        pending,
        synced,
        failed,
    );

    if let Some(rss_end) = process_rss_bytes() {
        observability::record_sync_cycle_rss_bytes(blockchain_id, "end", rss_end);
        if let Some(rss_start) = rss_start {
            let delta = rss_end as i64 - rss_start as i64;
            observability::record_sync_cycle_rss_delta_bytes(blockchain_id, delta);
        }
    }
}

#[cfg(target_os = "linux")]
fn process_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let rss_kib = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u64>().ok())?;
    Some(rss_kib.saturating_mul(1024))
}

#[cfg(not(target_os = "linux"))]
fn process_rss_bytes() -> Option<u64> {
    None
}
