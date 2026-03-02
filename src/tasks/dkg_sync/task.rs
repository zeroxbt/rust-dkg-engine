use std::{collections::HashSet, sync::Arc, time::Duration};

use chrono::Utc;
use dkg_blockchain::{Address, BlockchainId, ContractName};
use dkg_domain::canonical_evm_address;
use dkg_observability as observability;
use futures::StreamExt;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    DkgSyncConfig,
    discovery::DiscoveryWorker,
    pipeline::{
        DkgSyncPipeline,
        types::{QueueKcKey, QueueOutcome},
    },
    queue::{dispatcher, outcomes},
};
use crate::tasks::periodic::DkgSyncDeps;

const SYNC_RETRY_DELAY_SECS: i64 = 60;

pub(crate) struct DkgSyncTask {
    config: DkgSyncConfig,
    deps: DkgSyncDeps,
    notify: Arc<Notify>,
    inflight: Arc<Mutex<HashSet<QueueKcKey>>>,
}

impl DkgSyncTask {
    pub(crate) fn new(deps: DkgSyncDeps, config: DkgSyncConfig) -> Self {
        Self {
            config,
            deps,
            notify: Arc::new(Notify::new()),
            inflight: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        tracing::info!(
            blockchain_id = %blockchain_id,
            discovery_enabled = self.config.discovery.enabled,
            "Starting DKG sync queue processor"
        );

        observability::record_sync_pipeline_inflight(0);

        let sync_pipeline = DkgSyncPipeline::new(self.deps.clone(), self.config.clone());
        let (outcome_tx, mut outcome_rx) = mpsc::channel::<Vec<QueueOutcome>>(
            self.config.queue_processor.pipeline_channel_buffer.max(1) * 2,
        );
        let pipeline_runtime = sync_pipeline.start(blockchain_id, outcome_tx);

        let discovery_handle = self
            .start_discovery_if_enabled(blockchain_id, shutdown.clone())
            .await;
        let fallback_retry_poll =
            Duration::from_secs(self.config.queue_processor.dispatch_idle_poll_secs.max(1));
        let mut draining = false;
        let mut pipeline_runtime = Some(pipeline_runtime);

        loop {
            while let Ok(outcomes) = outcome_rx.try_recv() {
                outcomes::apply_queue_outcomes(
                    &self.deps,
                    &self.inflight,
                    self.notify.as_ref(),
                    blockchain_id,
                    outcomes,
                    SYNC_RETRY_DELAY_SECS,
                )
                .await;
            }

            if shutdown.is_cancelled() && !draining {
                draining = true;
                tracing::info!(
                    blockchain_id = %blockchain_id,
                    "DKG sync queue processor entering draining mode"
                );
            }

            let inflight_count = self.inflight_count().await;
            observability::record_sync_pipeline_inflight(inflight_count);

            if draining && inflight_count == 0 {
                tracing::info!(
                    blockchain_id = %blockchain_id,
                    "DKG sync queue processor drained in-flight work"
                );
                break;
            }

            let mut dispatched = false;
            if !draining
                && sync_pipeline.enough_peers_for_fetch(blockchain_id)
                && let Some(runtime) = pipeline_runtime.as_ref()
            {
                dispatched = dispatcher::dispatch_due_fifo(
                    &self.config,
                    &self.deps,
                    &self.inflight,
                    &runtime.input_tx,
                    blockchain_id,
                )
                .await;
            }

            if dispatched {
                continue;
            }

            tokio::select! {
                _ = shutdown.cancelled(), if !draining => {}
                maybe_outcomes = outcome_rx.recv() => {
                    if let Some(outcomes) = maybe_outcomes {
                        outcomes::apply_queue_outcomes(
                            &self.deps,
                            &self.inflight,
                            self.notify.as_ref(),
                            blockchain_id,
                            outcomes,
                            SYNC_RETRY_DELAY_SECS,
                        )
                        .await;
                    }
                }
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(fallback_retry_poll) => {}
            }
        }

        if let Some(runtime) = pipeline_runtime.take() {
            runtime.shutdown().await;
        }

        if let Some(discovery_handle) = discovery_handle
            && let Err(error) = discovery_handle.await
        {
            tracing::error!(error = ?error, "DKG sync discovery panicked");
        }

        observability::record_sync_pipeline_inflight(0);
        tracing::info!(blockchain_id = %blockchain_id, "DKG sync queue processor stopped");
    }

    async fn start_discovery_if_enabled(
        &self,
        blockchain_id: &BlockchainId,
        shutdown: CancellationToken,
    ) -> Option<tokio::task::JoinHandle<()>> {
        if !self.config.discovery.enabled {
            tracing::info!(
                blockchain_id = %blockchain_id,
                "DKG sync discovery is disabled"
            );
            return None;
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
                    "Failed to get KC storage contract addresses for DKG sync discovery"
                );
                return None;
            }
        };

        if contract_addresses.is_empty() {
            tracing::info!(
                blockchain_id = %blockchain_id,
                "No KC storage contracts found for DKG sync discovery"
            );
            return None;
        }

        let pinned_tip = match self
            .deps
            .blockchain_manager
            .get_block_number(blockchain_id)
            .await
        {
            Ok(block) => block.saturating_sub(self.config.discovery.head_safety_blocks),
            Err(error) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to pin DKG sync discovery tip"
                );
                return None;
            }
        };

        tracing::info!(
            blockchain_id = %blockchain_id,
            contracts = contract_addresses.len(),
            pinned_tip,
            "Starting DKG sync discovery"
        );

        Some(tokio::spawn(Self::discovery_loop(
            DiscoveryWorker::new(self.deps.clone(), self.config.clone()),
            self.deps.clone(),
            self.config.clone(),
            blockchain_id.clone(),
            contract_addresses,
            pinned_tip,
            Arc::clone(&self.notify),
            shutdown,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    async fn discovery_loop(
        discovery_worker: DiscoveryWorker,
        deps: DkgSyncDeps,
        config: DkgSyncConfig,
        blockchain_id: BlockchainId,
        contract_addresses: Vec<Address>,
        pinned_tip: u64,
        notify: Arc<Notify>,
        shutdown: CancellationToken,
    ) {
        let contract_scan_concurrency = config.discovery.max_contract_concurrency.max(1);
        let high_watermark = config.discovery.queue_high_watermark.max(1);
        let low_watermark = config.discovery.queue_low_watermark.max(1);
        let recheck_period =
            Duration::from_secs(config.discovery.metadata_error_retry_interval_secs.max(1));

        let mut completed_contracts: HashSet<String> = HashSet::new();

        loop {
            if shutdown.is_cancelled() {
                break;
            }

            let queue_total = Self::active_queue_count_static(
                &deps,
                config.queue_processor.max_retry_attempts,
                &blockchain_id,
            )
            .await;

            if queue_total >= high_watermark {
                loop {
                    if shutdown.is_cancelled() {
                        return;
                    }
                    let current_total = Self::active_queue_count_static(
                        &deps,
                        config.queue_processor.max_retry_attempts,
                        &blockchain_id,
                    )
                    .await;
                    if current_total <= low_watermark {
                        break;
                    }
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            return;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
            }

            let mut pending_contracts = Vec::new();
            for &contract_address in &contract_addresses {
                let contract_addr_str = canonical_evm_address(&contract_address);
                if !completed_contracts.contains(&contract_addr_str) {
                    pending_contracts.push(contract_address);
                }
            }

            if pending_contracts.is_empty() {
                return;
            }

            let mut any_chunk_processed = false;
            let results: Vec<(Address, Result<_, _>)> =
                futures::stream::iter(pending_contracts.into_iter().map(|contract_address| {
                    let discovery_worker = &discovery_worker;
                    let blockchain_id = &blockchain_id;
                    async move {
                        (
                            contract_address,
                            discovery_worker
                                .discover_contract_once(blockchain_id, contract_address, pinned_tip)
                                .await,
                        )
                    }
                }))
                .buffer_unordered(contract_scan_concurrency)
                .collect()
                .await;

            for (contract_address, result) in results {
                let contract_addr_str = canonical_evm_address(&contract_address);
                match result {
                    Ok(discovery_result) => {
                        if discovery_result.chunk_processed {
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
                            "DKG sync discovery failed for contract"
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
    }

    async fn inflight_count(&self) -> usize {
        self.inflight.lock().await.len()
    }

    async fn active_queue_count_static(
        deps: &DkgSyncDeps,
        max_retry_attempts: u32,
        blockchain_id: &BlockchainId,
    ) -> u64 {
        let now_ts = Utc::now().timestamp();
        let due = deps
            .kc_sync_repository
            .count_queue_due_for_blockchain(blockchain_id.as_str(), now_ts, max_retry_attempts)
            .await;
        let retrying = deps
            .kc_sync_repository
            .count_queue_retrying_for_blockchain(blockchain_id.as_str(), now_ts, max_retry_attempts)
            .await;

        match (due, retrying) {
            (Ok(due), Ok(retrying)) => due.saturating_add(retrying),
            (due_result, retry_result) => {
                if let Err(error) = due_result {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        error = %error,
                        "Failed to read due queue count in DKG sync task"
                    );
                }
                if let Err(error) = retry_result {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        error = %error,
                        "Failed to read retry queue count in DKG sync task"
                    );
                }
                0
            }
        }
    }
}
