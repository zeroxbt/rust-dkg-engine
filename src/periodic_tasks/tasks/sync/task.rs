//! Data sync task orchestration (queue consumer).

use std::{sync::Arc, time::Duration};

use dkg_blockchain::{Address, BlockchainId, ContractName};
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
use crate::{periodic_tasks::SyncDeps, periodic_tasks::runner::run_with_shutdown};

pub(crate) struct SyncTask {
    config: SyncConfig,
    deps: SyncDeps,
}

#[derive(Debug, Error)]
enum SyncTaskError {
    #[error("Failed to fetch due KC IDs from queue")]
    FetchDueKcIds(#[source] dkg_repository::error::RepositoryError),
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
                pending: 0,
                synced: 0,
                failed: 0,
            });
        }

        if !allow_pipeline_fetch {
            return Ok(ContractSyncResult {
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
        let idle_period = Duration::from_secs(self.config.sync_idle_sleep_secs.max(1));
        let hot_loop_period = Duration::from_millis(500);

        if !self.config.enabled {
            return idle_period;
        }

        if let Err(error) = self
            .deps
            .blockchain_manager
            .get_block_number(blockchain_id)
            .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                error = %error,
                "Failed to resolve current block for data sync cycle"
            );
            return idle_period;
        }
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
                return idle_period;
            }
        };

        let sync_futures = contract_addresses.iter().map(|&contract_address| {
            self.sync_contract(blockchain_id, contract_address, enough_peers)
        });
        let results = futures::future::join_all(sync_futures).await;

        let mut total_pending = 0_usize;
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(r) => {
                    total_pending += r.pending;

                    if r.pending > 0 {
                        tracing::trace!(
                            contract = ?contract_addresses[i],
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

        if !enough_peers || total_pending == 0 {
            return idle_period;
        } else {
            return hot_loop_period;
        }
    }
}
