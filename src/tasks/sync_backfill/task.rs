use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use dkg_blockchain::{Address, BlockchainId, ContractName};
use dkg_observability as observability;
use futures::StreamExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::{SyncConfig, metadata_replenisher::MetadataReplenisher, pipeline::SyncPipeline};
use crate::tasks::periodic::SyncDeps;

pub(crate) struct SyncBackfillTask {
    config: SyncConfig,
    deps: SyncDeps,
    notify: Arc<Notify>,
}

impl SyncBackfillTask {
    pub(crate) fn new(deps: SyncDeps, config: SyncConfig) -> Self {
        Self {
            config,
            deps,
            notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        if !self.config.enabled {
            return;
        }

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
                    "Failed to get KC storage contract addresses for sync backfill task"
                );
                return;
            }
        };

        if contract_addresses.is_empty() {
            tracing::info!(
                blockchain_id = %blockchain_id,
                "No KC storage contracts found for sync backfill task"
            );
            return;
        }

        let contract_address_by_str: HashMap<String, Address> = contract_addresses
            .iter()
            .map(|address| (format!("{:?}", address), *address))
            .collect();

        let pinned_tip = match self
            .deps
            .blockchain_manager
            .get_block_number(blockchain_id)
            .await
        {
            Ok(block) => block.saturating_sub(self.config.head_safety_blocks),
            Err(error) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to pin backfill tip for sync backfill task"
                );
                return;
            }
        };

        tracing::info!(
            blockchain_id = %blockchain_id,
            contracts = contract_addresses.len(),
            pinned_tip,
            "Starting sync backfill task"
        );

        observability::record_sync_pipeline_mode("running");
        observability::record_sync_pipeline_inflight(0);

        let sync_pipeline = SyncPipeline::new(self.deps.clone(), self.config.clone());

        let replenisher_done = Arc::new(AtomicBool::new(false));
        let replenisher_handle = tokio::spawn(Self::replenisher_loop(
            MetadataReplenisher::new(self.deps.clone(), self.config.clone()),
            self.deps.clone(),
            self.config.clone(),
            blockchain_id.clone(),
            contract_addresses,
            pinned_tip,
            Arc::clone(&self.notify),
            Arc::clone(&replenisher_done),
            shutdown.clone(),
        ));

        let fallback_retry_poll = Duration::from_secs(self.config.no_peers_retry_delay_secs.max(1));
        let mut draining = false;

        loop {
            if shutdown.is_cancelled() && !draining {
                draining = true;
                observability::record_sync_pipeline_mode("draining");
                observability::record_sync_pipeline_dispatch_wake("shutdown");
                tracing::info!(
                    blockchain_id = %blockchain_id,
                    "Sync backfill task entering draining mode"
                );
            }

            let queue_total = self.active_queue_count(blockchain_id).await;

            if replenisher_done.load(Ordering::Acquire) && queue_total == 0 {
                tracing::info!(
                    blockchain_id = %blockchain_id,
                    "Sync backfill catch-up completed"
                );
                break;
            }

            // In drain mode we stop claiming new work and exit.
            if draining {
                tracing::info!(
                    blockchain_id = %blockchain_id,
                    "Sync backfill drained in-flight work"
                );
                break;
            }

            let mut processed = false;
            if sync_pipeline.enough_peers_for_fetch(blockchain_id) {
                processed = self
                    .process_due_fifo_batches(
                        &sync_pipeline,
                        blockchain_id,
                        &contract_address_by_str,
                    )
                    .await;
            }

            if processed {
                continue;
            }

            tokio::select! {
                _ = shutdown.cancelled() => {
                    observability::record_sync_pipeline_dispatch_wake("shutdown");
                }
                _ = self.notify.notified() => {
                    observability::record_sync_pipeline_dispatch_wake("notify");
                }
                _ = tokio::time::sleep(fallback_retry_poll) => {
                    observability::record_sync_pipeline_dispatch_wake("timer");
                }
            }
        }

        if let Err(error) = replenisher_handle.await {
            tracing::error!(error = ?error, "Sync backfill replenisher panicked");
        }

        observability::record_sync_pipeline_inflight(0);
        observability::record_sync_pipeline_mode("completed");
        tracing::info!(blockchain_id = %blockchain_id, "Sync backfill task stopped");
    }

    #[allow(clippy::too_many_arguments)]
    async fn replenisher_loop(
        metadata_replenisher: MetadataReplenisher,
        deps: SyncDeps,
        config: SyncConfig,
        blockchain_id: BlockchainId,
        contract_addresses: Vec<Address>,
        pinned_tip: u64,
        notify: Arc<Notify>,
        done: Arc<AtomicBool>,
        shutdown: CancellationToken,
    ) {
        let capacity = config.pipeline_capacity.max(1);
        let high_watermark = (capacity * 3) as u64;
        let low_watermark = (capacity * 2) as u64;
        let recheck_period = Duration::from_secs(config.metadata_gap_recheck_interval_secs.max(1));

        let mut completed_contracts: HashSet<String> = HashSet::new();

        loop {
            if shutdown.is_cancelled() {
                break;
            }

            let queue_total = Self::active_queue_count_static(&deps, &config, &blockchain_id).await;

            if queue_total >= high_watermark {
                loop {
                    if shutdown.is_cancelled() {
                        done.store(true, Ordering::Release);
                        return;
                    }
                    let current_total =
                        Self::active_queue_count_static(&deps, &config, &blockchain_id).await;
                    if current_total <= low_watermark {
                        break;
                    }
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            done.store(true, Ordering::Release);
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }

            let mut pending_contracts = Vec::new();
            for &contract_address in &contract_addresses {
                let contract_addr_str = format!("{:?}", contract_address);
                if !completed_contracts.contains(&contract_addr_str) {
                    pending_contracts.push(contract_address);
                }
            }

            if pending_contracts.is_empty() {
                done.store(true, Ordering::Release);
                return;
            }

            let mut any_chunk_processed = false;
            let results: Vec<(Address, Result<_, _>)> =
                futures::stream::iter(pending_contracts.into_iter().map(|contract_address| {
                    let metadata_replenisher = &metadata_replenisher;
                    let blockchain_id = &blockchain_id;
                    async move {
                        (
                            contract_address,
                            metadata_replenisher
                                .replenish_contract_once(
                                    blockchain_id,
                                    contract_address,
                                    pinned_tip,
                                )
                                .await,
                        )
                    }
                }))
                .buffer_unordered(capacity)
                .collect()
                .await;

            for (contract_address, result) in results {
                let contract_addr_str = format!("{:?}", contract_address);
                match result {
                    Ok(replenish_result) => {
                        if replenish_result.chunk_processed {
                            any_chunk_processed = true;
                            notify.notify_waiters();
                        } else {
                            completed_contracts.insert(contract_addr_str);
                        }
                    }
                    Err(error) => {
                        tracing::error!(
                            blockchain_id = %blockchain_id,
                            contract = %contract_addr_str,
                            error = %error,
                            "Sync backfill replenisher failed for contract"
                        );
                    }
                }
            }

            if !any_chunk_processed {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(recheck_period) => {}
                }
            }
        }

        done.store(true, Ordering::Release);
    }

    async fn process_due_fifo_batches(
        &self,
        sync_pipeline: &SyncPipeline,
        blockchain_id: &BlockchainId,
        contract_address_by_str: &HashMap<String, Address>,
    ) -> bool {
        let now_ts = Utc::now().timestamp();
        let limit = self.config.pipeline_capacity.max(1) as u64;

        let due_rows = match self
            .deps
            .kc_sync_repository
            .get_due_queue_entries_fifo_for_blockchain(
                blockchain_id.as_str(),
                now_ts,
                self.config.max_retry_attempts,
                limit,
            )
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to read due sync queue rows"
                );
                return false;
            }
        };

        if due_rows.is_empty() {
            return false;
        }

        let mut processed_any = false;
        for row in due_rows {
            let Some(&contract_address) = contract_address_by_str.get(&row.contract_address) else {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    contract = %row.contract_address,
                    kc_id = row.kc_id,
                    "Skipping queue row for unknown contract"
                );
                continue;
            };

            processed_any = true;
            observability::record_sync_pipeline_inflight(1);
            match sync_pipeline
                .process_contract_batch(blockchain_id, contract_address, vec![row.kc_id])
                .await
            {
                Ok(outcome) => {
                    tracing::trace!(
                        blockchain_id = %blockchain_id,
                        contract = %row.contract_address,
                        kc_id = row.kc_id,
                        pending = outcome.pending,
                        synced = outcome.synced,
                        failed = outcome.failed,
                        "Sync backfill processed contract batch"
                    );
                }
                Err(error) => {
                    tracing::error!(
                        blockchain_id = %blockchain_id,
                        contract = %row.contract_address,
                        kc_id = row.kc_id,
                        error = %error,
                        "Sync backfill failed to process contract batch"
                    );
                }
            }
            observability::record_sync_pipeline_inflight(0);
        }

        if processed_any {
            observability::record_sync_pipeline_dispatch_wake("batch_processed");
        }
        processed_any
    }

    async fn active_queue_count(&self, blockchain_id: &BlockchainId) -> u64 {
        Self::active_queue_count_static(&self.deps, &self.config, blockchain_id).await
    }

    async fn active_queue_count_static(
        deps: &SyncDeps,
        config: &SyncConfig,
        blockchain_id: &BlockchainId,
    ) -> u64 {
        let now_ts = Utc::now().timestamp();
        let due = deps
            .kc_sync_repository
            .count_queue_due_for_blockchain(
                blockchain_id.as_str(),
                now_ts,
                config.max_retry_attempts,
            )
            .await;
        let retrying = deps
            .kc_sync_repository
            .count_queue_retrying_for_blockchain(
                blockchain_id.as_str(),
                now_ts,
                config.max_retry_attempts,
            )
            .await;

        match (due, retrying) {
            (Ok(due), Ok(retrying)) => due.saturating_add(retrying),
            (due_result, retry_result) => {
                if let Err(error) = due_result {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        error = %error,
                        "Failed to read due queue count in sync backfill task"
                    );
                }
                if let Err(error) = retry_result {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        error = %error,
                        "Failed to read retry queue count in sync backfill task"
                    );
                }
                0
            }
        }
    }
}
