//! Pipeline processor for the sync backfill task workers.

use std::sync::Arc;

use dkg_blockchain::{Address, BlockchainId};
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::Instrument;

use super::{
    SyncConfig,
    fetch::run_fetch_stage,
    filter::run_filter_stage,
    insert::run_insert_stage,
    types::{ContractSyncResult, FetchStats, FetchedKc, FilterStats, InsertStats, KcToSync},
};
use crate::tasks::periodic::SyncDeps;

// Keep peer fanout fixed to avoid overloading the network during backfill.
const FETCH_FANOUT_CONCURRENCY: usize = 3;

pub(crate) struct SyncPipeline {
    config: SyncConfig,
    deps: SyncDeps,
}

#[derive(Debug, Error)]
pub(crate) enum SyncPipelineError {
    #[error("Filter stage panicked")]
    Filter(#[source] tokio::task::JoinError),
    #[error("Fetch stage panicked")]
    Fetch(#[source] tokio::task::JoinError),
    #[error("Insert stage panicked")]
    Insert(#[source] tokio::task::JoinError),
}

impl SyncPipeline {
    pub(crate) fn new(deps: SyncDeps, config: SyncConfig) -> Self {
        Self { config, deps }
    }

    pub(crate) fn enough_peers_for_fetch(&self, blockchain_id: &BlockchainId) -> bool {
        let total_shard_peers = self.deps.peer_registry.shard_peer_count(blockchain_id);
        let identified_peers = self
            .deps
            .peer_registry
            .identified_shard_peer_count(blockchain_id);
        let min_required = (total_shard_peers / 3).max(3);
        identified_peers >= min_required
    }

    #[tracing::instrument(
        skip(self, due_kc_ids),
        fields(blockchain_id = %blockchain_id, contract = %format!("{:?}", contract_address), pending = due_kc_ids.len())
    )]
    pub(crate) async fn process_contract_batch(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        due_kc_ids: Vec<u64>,
    ) -> Result<ContractSyncResult, SyncPipelineError> {
        let contract_addr_str = format!("{:?}", contract_address);
        let pending = due_kc_ids.len();

        if pending == 0 {
            return Ok(ContractSyncResult {
                pending: 0,
                synced: 0,
                failed: 0,
            });
        }

        let (filter_stats, fetch_stats, insert_stats) = self
            .run_pipeline(
                due_kc_ids,
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
            kcs_already_synced = filter_stats.already_synced.len(),
            kcs_expired = filter_stats.expired.len(),
            kcs_waiting_for_metadata = filter_stats.waiting_for_metadata.len(),
            kcs_waiting_for_state = filter_stats.waiting_for_state.len(),
            kcs_fetch_failed = fetch_failures_count,
            kcs_synced = insert_stats.synced.len(),
            kcs_insert_failed = insert_failures_count,
            "Sync pipeline contract summary"
        );

        Ok(ContractSyncResult {
            pending,
            synced,
            failed,
        })
    }

    async fn run_pipeline(
        &self,
        pending_kc_ids: Vec<u64>,
        blockchain_id: BlockchainId,
        contract_address: Address,
        contract_addr_str: String,
    ) -> Result<(FilterStats, FetchStats, InsertStats), SyncPipelineError> {
        let pipeline_capacity = self.config.pipeline_capacity.max(1);
        let pipeline_channel_buffer = self.config.pipeline_channel_buffer.max(1);
        let insert_batch_concurrency = self.config.insert_batch_concurrency.max(1);
        let max_assets_per_fetch_batch = self.config.max_assets_per_fetch_batch.max(1);

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
                    run_filter_stage(
                        pending_kc_ids,
                        pipeline_capacity,
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
                    run_fetch_stage(
                        filter_rx,
                        pipeline_capacity,
                        FETCH_FANOUT_CONCURRENCY,
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
                    run_insert_stage(
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

        let filter_stats = filter_result.map_err(SyncPipelineError::Filter)?;
        let fetch_stats = fetch_result.map_err(SyncPipelineError::Fetch)?;
        let insert_stats = insert_result.map_err(SyncPipelineError::Insert)?;

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
