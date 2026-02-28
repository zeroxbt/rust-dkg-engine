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
use tokio::sync::{Mutex, Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    SyncConfig,
    metadata_replenisher::MetadataReplenisher,
    pipeline::SyncPipeline,
    types::{QueueKcWorkItem, QueueOutcome, QueueOutcomeKind},
};
use crate::tasks::periodic::SyncDeps;

const SYNC_RETRY_DELAY_SECS: i64 = 60;

pub(crate) struct SyncBackfillTask {
    config: SyncConfig,
    deps: SyncDeps,
    notify: Arc<Notify>,
    inflight: Arc<Mutex<HashSet<(String, u64)>>>,
}

impl SyncBackfillTask {
    pub(crate) fn new(deps: SyncDeps, config: SyncConfig) -> Self {
        Self {
            config,
            deps,
            notify: Arc::new(Notify::new()),
            inflight: Arc::new(Mutex::new(HashSet::new())),
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

        observability::record_sync_pipeline_inflight(0);

        let current_epoch = match self
            .deps
            .blockchain_manager
            .get_current_epoch(blockchain_id)
            .await
        {
            Ok(epoch) => Some(epoch),
            Err(error) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to snapshot current epoch for sync filter stage"
                );
                None
            }
        };

        let sync_pipeline = SyncPipeline::new(self.deps.clone(), self.config.clone());
        let (outcome_tx, mut outcome_rx) =
            mpsc::channel::<Vec<QueueOutcome>>(self.config.stage_channel_buffer.max(1) * 2);
        let pipeline_runtime = sync_pipeline.start(blockchain_id, current_epoch, outcome_tx);

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

        let fallback_retry_poll = Duration::from_secs(self.config.dispatch_idle_poll_secs.max(1));
        let mut draining = false;
        let mut pipeline_runtime = Some(pipeline_runtime);

        loop {
            while let Ok(outcomes) = outcome_rx.try_recv() {
                self.apply_queue_outcomes(blockchain_id, outcomes).await;
            }

            if shutdown.is_cancelled() && !draining {
                draining = true;
                tracing::info!(
                    blockchain_id = %blockchain_id,
                    "Sync backfill task entering draining mode"
                );
            }

            let queue_total = self.active_queue_count(blockchain_id).await;
            let inflight_count = self.inflight_count().await;
            observability::record_sync_pipeline_inflight(inflight_count);

            if replenisher_done.load(Ordering::Acquire) && queue_total == 0 && inflight_count == 0 {
                tracing::info!(
                    blockchain_id = %blockchain_id,
                    "Sync backfill catch-up completed"
                );
                break;
            }

            if draining && inflight_count == 0 {
                tracing::info!(
                    blockchain_id = %blockchain_id,
                    "Sync backfill drained in-flight work"
                );
                break;
            }

            let mut dispatched = false;
            if !draining
                && sync_pipeline.enough_peers_for_fetch(blockchain_id)
                && let Some(runtime) = pipeline_runtime.as_ref()
            {
                dispatched = self
                    .dispatch_due_fifo(&runtime.input_tx, blockchain_id, &contract_address_by_str)
                    .await;
            }

            if dispatched {
                continue;
            }

            tokio::select! {
                _ = shutdown.cancelled(), if !draining => {}
                maybe_outcomes = outcome_rx.recv() => {
                    if let Some(outcomes) = maybe_outcomes {
                        self.apply_queue_outcomes(blockchain_id, outcomes).await;
                    }
                }
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(fallback_retry_poll) => {}
            }
        }

        if let Some(runtime) = pipeline_runtime.take() {
            runtime.shutdown().await;
        }

        if let Err(error) = replenisher_handle.await {
            tracing::error!(error = ?error, "Sync backfill replenisher panicked");
        }

        observability::record_sync_pipeline_inflight(0);
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
        let high_watermark = config.queue_high_watermark.max(1);
        let low_watermark = config.queue_low_watermark.max(1);
        let recheck_period = Duration::from_secs(config.metadata_error_retry_interval_secs.max(1));

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

    async fn dispatch_due_fifo(
        &self,
        input_tx: &mpsc::Sender<Vec<QueueKcWorkItem>>,
        blockchain_id: &BlockchainId,
        contract_address_by_str: &HashMap<String, Address>,
    ) -> bool {
        let free_slots = self
            .config
            .pipeline_capacity
            .max(1)
            .saturating_sub(self.inflight_count().await);
        if free_slots == 0 {
            return false;
        }

        let now_ts = Utc::now().timestamp();
        let due_rows = match self
            .deps
            .kc_sync_repository
            .get_due_queue_entries_fifo_for_blockchain(
                blockchain_id.as_str(),
                now_ts,
                self.config.max_retry_attempts,
                free_slots as u64,
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

        let mut accepted = Vec::with_capacity(due_rows.len());
        {
            let mut inflight = self.inflight.lock().await;
            for row in due_rows {
                let Some(&contract_address) = contract_address_by_str.get(&row.contract_address)
                else {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %row.contract_address,
                        kc_id = row.kc_id,
                        "Skipping queue row for unknown contract"
                    );
                    continue;
                };

                let key = (row.contract_address.clone(), row.kc_id);
                if inflight.insert(key) {
                    accepted.push(QueueKcWorkItem {
                        contract_address,
                        contract_addr_str: row.contract_address,
                        kc_id: row.kc_id,
                    });
                }
            }
            observability::record_sync_pipeline_inflight(inflight.len());
        }

        if accepted.is_empty() {
            return false;
        }

        let rollback_keys: Vec<(String, u64)> = accepted
            .iter()
            .map(|item| (item.contract_addr_str.clone(), item.kc_id))
            .collect();

        if input_tx.send(accepted).await.is_err() {
            tracing::warn!("Sync pipeline input channel closed");
            let mut inflight = self.inflight.lock().await;
            for key in rollback_keys {
                inflight.remove(&key);
            }
            observability::record_sync_pipeline_inflight(inflight.len());
            return false;
        }

        true
    }

    async fn apply_queue_outcomes(
        &self,
        blockchain_id: &BlockchainId,
        outcomes: Vec<QueueOutcome>,
    ) {
        if outcomes.is_empty() {
            return;
        }

        let mut remove_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
        let mut retry_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
        for outcome in &outcomes {
            match outcome.kind {
                QueueOutcomeKind::Remove => remove_by_contract
                    .entry(outcome.contract_addr_str.clone())
                    .or_default()
                    .push(outcome.kc_id),
                QueueOutcomeKind::Retry => retry_by_contract
                    .entry(outcome.contract_addr_str.clone())
                    .or_default()
                    .push(outcome.kc_id),
            }
        }

        for (contract, mut kc_ids) in remove_by_contract {
            kc_ids.sort_unstable();
            kc_ids.dedup();
            if let Err(error) = self
                .deps
                .kc_sync_repository
                .remove_kcs(blockchain_id.as_str(), &contract, &kc_ids)
                .await
            {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract,
                    error = %error,
                    "Failed to remove KCs from queue"
                );
            }
        }

        for (contract, mut kc_ids) in retry_by_contract {
            kc_ids.sort_unstable();
            kc_ids.dedup();
            if let Err(error) = self
                .deps
                .kc_sync_repository
                .increment_retry_count(
                    blockchain_id.as_str(),
                    &contract,
                    &kc_ids,
                    SYNC_RETRY_DELAY_SECS,
                )
                .await
            {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract,
                    error = %error,
                    "Failed to increment retry count"
                );
            }
        }

        {
            let mut inflight = self.inflight.lock().await;
            for outcome in outcomes {
                inflight.remove(&(outcome.contract_addr_str, outcome.kc_id));
            }
            observability::record_sync_pipeline_inflight(inflight.len());
        }

        self.notify.notify_waiters();
    }

    async fn inflight_count(&self) -> usize {
        self.inflight.lock().await.len()
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
