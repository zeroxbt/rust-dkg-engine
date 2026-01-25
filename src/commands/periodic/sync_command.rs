use std::{sync::Arc, time::Duration};

use futures::future::join_all;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::{Address, BlockchainId, BlockchainManager, ContractName},
        repository::RepositoryManager,
    },
};

/// Interval between sync cycles (30 seconds)
const SYNC_PERIOD: Duration = Duration::from_secs(30);

/// Maximum number of new KCs to enqueue per contract per cycle
const MAX_NEW_KCS_PER_CONTRACT: u64 = 1000;

/// Maximum number of KCs to process per sync cycle per contract
#[allow(dead_code)]
const SYNC_BATCH_SIZE: u64 = 1000;

/// Maximum retry attempts before marking a KC as permanently failed
#[allow(dead_code)]
const MAX_RETRY_ATTEMPTS: u32 = 2;

pub(crate) struct SyncCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
}

impl SyncCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
        }
    }

    /// Check for new KCs on a single contract and enqueue any that need syncing.
    async fn check_contract_for_new_kcs(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
    ) -> Result<u64, String> {
        let contract_addr_str = format!("{:?}", contract_address);

        // Get latest KC ID from chain
        let latest_on_chain = self
            .blockchain_manager
            .get_latest_knowledge_collection_id(blockchain_id, contract_address)
            .await
            .map_err(|e| format!("Failed to get latest KC ID: {}", e))?;

        // Get our last checked ID from DB
        let last_checked = self
            .repository_manager
            .kc_sync_repository()
            .get_progress(blockchain_id.as_str(), &contract_addr_str)
            .await
            .map_err(|e| format!("Failed to get sync progress: {}", e))?
            .map(|p| p.last_checked_id)
            .unwrap_or(0);

        // Nothing new to check
        if latest_on_chain <= last_checked {
            return Ok(0);
        }

        // Calculate range of new KC IDs to enqueue (limited to avoid huge batches)
        let start_id = last_checked + 1;
        let end_id = std::cmp::min(latest_on_chain, last_checked + MAX_NEW_KCS_PER_CONTRACT);
        let new_kc_ids: Vec<u64> = (start_id..=end_id).collect();
        let count = new_kc_ids.len() as u64;

        // Enqueue the new KC IDs
        self.repository_manager
            .kc_sync_repository()
            .enqueue_kcs(blockchain_id.as_str(), &contract_addr_str, &new_kc_ids)
            .await
            .map_err(|e| format!("Failed to enqueue KCs: {}", e))?;

        // Update progress to the highest ID we've now checked
        self.repository_manager
            .kc_sync_repository()
            .upsert_progress(blockchain_id.as_str(), &contract_addr_str, end_id)
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

        // Get all KC storage contract addresses for this blockchain
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

        // Check each contract for new KCs and enqueue them (in parallel)
        let check_futures = contract_addresses.iter().map(|&contract_address| {
            self.check_contract_for_new_kcs(&data.blockchain_id, contract_address)
        });

        let results = join_all(check_futures).await;

        let mut total_enqueued = 0u64;
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(count) => {
                    total_enqueued += count;
                    if count > 0 {
                        tracing::debug!(
                            blockchain_id = %data.blockchain_id,
                            contract = ?contract_addresses[i],
                            enqueued = count,
                            "[DKG SYNC] Enqueued new KCs"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        blockchain_id = %data.blockchain_id,
                        contract = ?contract_addresses[i],
                        error = %e,
                        "[DKG SYNC] Failed to check contract for new KCs"
                    );
                }
            }
        }

        if total_enqueued > 0 {
            tracing::info!(
                blockchain_id = %data.blockchain_id,
                total_enqueued,
                "[DKG SYNC] Enqueued new KCs for syncing"
            );
        }

        // TODO 3: Fetch a batch of pending KCs from DB for this blockchain
        //         (limit SYNC_BATCH_SIZE, retry_count < MAX_RETRY_ATTEMPTS)

        // TODO 4: For each pending KC:
        //         a. Check if expired on chain - if so, remove from queue (don't count as retry)
        //         b. Check if we already have it locally in triple store - if so, remove from queue

        // TODO 5: For KCs not found locally and not expired, batch GET from network

        // TODO 6: Store fetched KCs in triple store

        // TODO 7: Update DB: mark successful KCs as synced, increment retry_count for failed ones

        // TODO 8: Update last synced KC ID in DB for each contract

        tracing::info!(
            blockchain_id = %data.blockchain_id,
            "[DKG SYNC] Sync cycle completed"
        );

        CommandExecutionResult::Repeat { delay: SYNC_PERIOD }
    }
}
