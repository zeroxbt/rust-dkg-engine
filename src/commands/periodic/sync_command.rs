//! Sync command: orchestrates the KC sync pipeline for each blockchain.
//!
//! This command is responsible for:
//! 1. Discovering KCs that need syncing from the blockchain
//! 2. Coordinating the three-stage pipeline (filter → fetch → insert)
//! 3. Updating the sync queue based on results

use std::{sync::Arc, time::Instant};

use futures::future::join_all;
use tokio::sync::mpsc;
use tracing::Instrument;

use super::sync::{
    ContractSyncResult, FetchedKc, KcToSync, MAX_NEW_KCS_PER_CONTRACT, MAX_RETRY_ATTEMPTS,
    PIPELINE_CHANNEL_BUFFER, SYNC_PERIOD, fetch_task, filter_task, insert_task,
};
use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::{Address, BlockchainId, BlockchainManager, ContractName},
        repository::RepositoryManager,
    },
    services::{GetValidationService, PeerPerformanceTracker, TripleStoreService},
};

pub(crate) struct SyncCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
    triple_store_service: Arc<TripleStoreService>,
    network_manager: Arc<crate::managers::network::NetworkManager>,
    get_validation_service: Arc<GetValidationService>,
    peer_performance_tracker: Arc<PeerPerformanceTracker>,
}

impl SyncCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            network_manager: Arc::clone(context.network_manager()),
            get_validation_service: Arc::clone(context.get_validation_service()),
            peer_performance_tracker: Arc::clone(context.peer_performance_tracker()),
        }
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
        let sync_start = Instant::now();

        // Step 1: Get pending KCs from queue
        let db_start = Instant::now();
        let mut pending_kcs = self
            .repository_manager
            .kc_sync_repository()
            .get_pending_kcs_for_contract(
                blockchain_id.as_str(),
                &contract_addr_str,
                MAX_RETRY_ATTEMPTS,
                MAX_NEW_KCS_PER_CONTRACT,
            )
            .await
            .map_err(|e| format!("Failed to fetch pending KCs: {}", e))?;
        let db_fetch_ms = db_start.elapsed().as_millis();

        // Step 2: Enqueue new KCs if needed
        let enqueue_start = Instant::now();
        let existing_count = pending_kcs.len() as u64;
        let enqueued = if existing_count < MAX_NEW_KCS_PER_CONTRACT {
            let needed = MAX_NEW_KCS_PER_CONTRACT - existing_count;
            let newly_enqueued = self
                .enqueue_new_kcs(blockchain_id, contract_address, &contract_addr_str, needed)
                .await?;

            if newly_enqueued > 0 {
                pending_kcs = self
                    .repository_manager
                    .kc_sync_repository()
                    .get_pending_kcs_for_contract(
                        blockchain_id.as_str(),
                        &contract_addr_str,
                        MAX_RETRY_ATTEMPTS,
                        MAX_NEW_KCS_PER_CONTRACT,
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
                "[DKG SYNC] Queue already has enough pending KCs, skipping enqueue"
            );
            0
        };
        let enqueue_ms = enqueue_start.elapsed().as_millis();

        let pending = pending_kcs.len();

        if pending == 0 {
            tracing::debug!(enqueue_ms, db_fetch_ms, "[DKG SYNC] No pending KCs");
            return Ok(ContractSyncResult {
                enqueued,
                pending: 0,
                synced: 0,
                failed: 0,
            });
        }

        let pending_kc_ids: Vec<u64> = pending_kcs.into_iter().map(|kc| kc.kc_id).collect();

        // Step 3: Run the pipeline
        let pipeline_start = Instant::now();
        let (filter_stats, fetch_stats, insert_stats) = self
            .run_pipeline(
                pending_kc_ids,
                blockchain_id.clone(),
                contract_address,
                contract_addr_str.clone(),
            )
            .await?;
        let pipeline_ms = pipeline_start.elapsed().as_millis();

        // Step 4: Update DB with results
        self.update_sync_queue(
            blockchain_id,
            &contract_addr_str,
            &filter_stats.already_synced,
            &filter_stats.expired,
            &insert_stats.synced,
            &fetch_stats.failures,
            &insert_stats.failed,
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
            enqueue_ms,
            db_fetch_ms,
            pipeline_ms,
            kcs_pending = pending,
            kcs_already_synced = filter_stats.already_synced.len(),
            kcs_expired = filter_stats.expired.len(),
            kcs_fetch_failed = fetch_failures_count,
            kcs_synced = insert_stats.synced.len(),
            kcs_insert_failed = insert_failures_count,
            "[DKG SYNC] Contract sync timing breakdown (pipelined)"
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
    ) -> Result<
        (
            super::sync::FilterStats,
            super::sync::FetchStats,
            super::sync::InsertStats,
        ),
        String,
    > {
        // Create pipeline channels
        let (filter_tx, filter_rx) = mpsc::channel::<Vec<KcToSync>>(PIPELINE_CHANNEL_BUFFER);
        let (fetch_tx, fetch_rx) = mpsc::channel::<Vec<FetchedKc>>(PIPELINE_CHANNEL_BUFFER);

        // Spawn filter task (with current span as parent for trace propagation)
        let filter_handle = {
            let blockchain_id = blockchain_id.clone();
            let contract_addr_str = contract_addr_str.clone();
            let blockchain_manager = Arc::clone(&self.blockchain_manager);
            let triple_store_service = Arc::clone(&self.triple_store_service);
            tokio::spawn(
                async move {
                    filter_task(
                        pending_kc_ids,
                        blockchain_id,
                        contract_address,
                        contract_addr_str,
                        blockchain_manager,
                        triple_store_service,
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
            let network_manager = Arc::clone(&self.network_manager);
            let repository_manager = Arc::clone(&self.repository_manager);
            let get_validation_service = Arc::clone(&self.get_validation_service);
            let peer_performance_tracker = Arc::clone(&self.peer_performance_tracker);
            tokio::spawn(
                async move {
                    fetch_task(
                        filter_rx,
                        blockchain_id,
                        network_manager,
                        repository_manager,
                        get_validation_service,
                        peer_performance_tracker,
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
            let triple_store_service = Arc::clone(&self.triple_store_service);
            tokio::spawn(
                async move {
                    insert_task(
                        fetch_rx,
                        blockchain_id,
                        contract_addr_str,
                        triple_store_service,
                    )
                    .await
                }
                .in_current_span(),
            )
        };

        // Wait for all tasks
        let (filter_result, fetch_result, insert_result) =
            tokio::join!(filter_handle, fetch_handle, insert_handle);

        let filter_stats = filter_result.map_err(|e| format!("Filter task panicked: {}", e))?;
        let fetch_stats = fetch_result.map_err(|e| format!("Fetch task panicked: {}", e))?;
        let insert_stats = insert_result.map_err(|e| format!("Insert task panicked: {}", e))?;

        Ok((filter_stats, fetch_stats, insert_stats))
    }

    /// Update the sync queue based on pipeline results.
    async fn update_sync_queue(
        &self,
        blockchain_id: &BlockchainId,
        contract_addr_str: &str,
        already_synced: &[u64],
        expired: &[u64],
        synced: &[u64],
        fetch_failures: &[u64],
        insert_failures: &[u64],
    ) {
        let repo = self.repository_manager.kc_sync_repository();

        // Remove already-synced KCs (found locally in filter stage)
        if !already_synced.is_empty()
            && let Err(e) = repo
                .remove_kcs(blockchain_id.as_str(), contract_addr_str, already_synced)
                .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                "[DKG SYNC] Failed to remove already-synced KCs from queue"
            );
        }

        // Remove expired KCs
        if !expired.is_empty()
            && let Err(e) = repo
                .remove_kcs(blockchain_id.as_str(), contract_addr_str, expired)
                .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                "[DKG SYNC] Failed to remove expired KCs from queue"
            );
        }

        // Remove successfully synced KCs
        if !synced.is_empty()
            && let Err(e) = repo
                .remove_kcs(blockchain_id.as_str(), contract_addr_str, synced)
                .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                "[DKG SYNC] Failed to remove synced KCs from queue"
            );
        }

        // Increment retry count for failed KCs
        let mut all_failed: Vec<u64> = fetch_failures.to_vec();
        all_failed.extend(insert_failures);

        if !all_failed.is_empty()
            && let Err(e) = repo
                .increment_retry_count(blockchain_id.as_str(), contract_addr_str, &all_failed)
                .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                "[DKG SYNC] Failed to increment retry count for failed KCs"
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
            "[DKG SYNC] Fetching latest KC ID from chain..."
        );

        let latest_on_chain = self
            .blockchain_manager
            .get_latest_knowledge_collection_id(blockchain_id, contract_address)
            .await
            .map_err(|e| format!("Failed to get latest KC ID: {}", e))?;

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            latest_on_chain,
            "[DKG SYNC] Got latest KC ID from chain"
        );

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            "[DKG SYNC] Fetching sync progress from DB..."
        );

        let last_checked = self
            .repository_manager
            .kc_sync_repository()
            .get_progress(blockchain_id.as_str(), contract_addr_str)
            .await
            .map_err(|e| format!("Failed to get sync progress: {}", e))?
            .map(|p| p.last_checked_id)
            .unwrap_or(0);

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            last_checked,
            "[DKG SYNC] Got sync progress from DB"
        );

        if latest_on_chain <= last_checked {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                "[DKG SYNC] No new KCs to enqueue (latest_on_chain <= last_checked)"
            );
            return Ok(0);
        }

        let start_id = last_checked + 1;
        let end_id = std::cmp::min(latest_on_chain, last_checked + limit);
        let new_kc_ids: Vec<u64> = (start_id..=end_id).collect();
        let count = new_kc_ids.len() as u64;

        self.repository_manager
            .kc_sync_repository()
            .enqueue_kcs(blockchain_id.as_str(), contract_addr_str, &new_kc_ids)
            .await
            .map_err(|e| format!("Failed to enqueue KCs: {}", e))?;

        self.repository_manager
            .kc_sync_repository()
            .upsert_progress(blockchain_id.as_str(), contract_addr_str, end_id)
            .await
            .map_err(|e| format!("Failed to update progress: {}", e))?;

        Ok(count)
    }
}

#[derive(Clone)]
pub(crate) struct SyncCommandData {
    pub blockchain_id: BlockchainId,
}

impl SyncCommandData {
    pub(crate) fn new(blockchain_id: BlockchainId) -> Self {
        Self { blockchain_id }
    }
}

impl CommandHandler<SyncCommandData> for SyncCommandHandler {
    async fn execute(&self, data: &SyncCommandData) -> CommandExecutionResult {
        tracing::info!(
            blockchain_id = %data.blockchain_id,
            "[DKG SYNC] Starting sync cycle"
        );

        let contract_addresses = match self
            .blockchain_manager
            .get_all_contract_addresses(
                &data.blockchain_id,
                &ContractName::KnowledgeCollectionStorage,
            )
            .await
        {
            Ok(addresses) => addresses,
            Err(e) => {
                tracing::error!(
                    blockchain_id = %data.blockchain_id,
                    error = %e,
                    "[DKG SYNC] Failed to get KC storage contract addresses"
                );
                return CommandExecutionResult::Repeat { delay: SYNC_PERIOD };
            }
        };

        tracing::debug!(
            blockchain_id = %data.blockchain_id,
            contract_count = contract_addresses.len(),
            "[DKG SYNC] Found KC storage contracts"
        );

        // Sync each contract in parallel
        let sync_futures = contract_addresses
            .iter()
            .map(|&contract_address| self.sync_contract(&data.blockchain_id, contract_address));

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
                        tracing::debug!(
                            blockchain_id = %data.blockchain_id,
                            contract = ?contract_addresses[i],
                            enqueued = r.enqueued,
                            pending = r.pending,
                            synced = r.synced,
                            failed = r.failed,
                            "[DKG SYNC] Contract sync completed"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        blockchain_id = %data.blockchain_id,
                        contract = ?contract_addresses[i],
                        error = %e,
                        "[DKG SYNC] Failed to sync contract"
                    );
                }
            }
        }

        if total_enqueued > 0 || total_pending > 0 {
            tracing::info!(
                blockchain_id = %data.blockchain_id,
                total_enqueued,
                total_pending,
                total_synced,
                total_failed,
                "[DKG SYNC] Sync cycle summary"
            );
        }

        tracing::info!(
            blockchain_id = %data.blockchain_id,
            "[DKG SYNC] Sync cycle completed"
        );

        CommandExecutionResult::Repeat { delay: SYNC_PERIOD }
    }
}
