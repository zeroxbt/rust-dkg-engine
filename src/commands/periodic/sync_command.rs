use std::{sync::Arc, time::Duration};

use futures::future::join_all;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::{Address, BlockchainId, BlockchainManager, ContractName},
        repository::RepositoryManager,
    },
    services::TripleStoreService,
    utils::ual::derive_ual,
};

/// Interval between sync cycles (30 seconds)
const SYNC_PERIOD: Duration = Duration::from_secs(30);

/// Maximum number of new KCs to enqueue per contract per cycle
const MAX_NEW_KCS_PER_CONTRACT: u64 = 1000;

/// Maximum retry attempts before a KC is no longer retried (stays in DB for future recovery)
const MAX_RETRY_ATTEMPTS: u32 = 2;

/// Maximum number of KCs per network batch GET request (dictated by receiver nodes)
#[allow(dead_code)]
const NETWORK_BATCH_SIZE: u64 = 1000;

pub(crate) struct SyncCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
    triple_store_service: Arc<TripleStoreService>,
}

/// Result of syncing a single contract
struct ContractSyncResult {
    enqueued: u64,
    pending: usize,
    synced: u64,
    failed: u64,
}

/// KC that needs to be fetched from the network
#[allow(dead_code)]
struct KcToSync {
    kc_id: u64,
    ual: String,
    start_token_id: u64,
    end_token_id: u64,
    burned: Vec<u64>,
}

impl SyncCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
        }
    }

    /// Sync a single contract: enqueue new KCs from chain, then process pending KCs.
    async fn sync_contract(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
    ) -> Result<ContractSyncResult, String> {
        let contract_addr_str = format!("{:?}", contract_address);

        // Step 1: Check for new KCs on chain and enqueue them
        let enqueued = self
            .enqueue_new_kcs(blockchain_id, contract_address, &contract_addr_str)
            .await?;

        // Step 2: Fetch pending KCs for this contract from DB
        let pending_kcs = self
            .repository_manager
            .kc_sync_repository()
            .get_pending_kcs_for_contract(
                blockchain_id.as_str(),
                &contract_addr_str,
                MAX_RETRY_ATTEMPTS,
            )
            .await
            .map_err(|e| format!("Failed to fetch pending KCs: {}", e))?;

        let pending_kc_ids: Vec<u64> = pending_kcs.into_iter().map(|kc| kc.kc_id).collect();
        let pending = pending_kc_ids.len();

        if pending == 0 {
            return Ok(ContractSyncResult {
                enqueued,
                pending: 0,
                synced: 0,
                failed: 0,
            });
        }

        // Step 3: Filter out expired and already-synced KCs
        let (kcs_to_sync, expired_kc_ids, already_synced_kc_ids) = self
            .filter_pending_kcs(blockchain_id, contract_address, &contract_addr_str, &pending_kc_ids)
            .await;

        // Remove expired KCs from queue (don't count as retry)
        if !expired_kc_ids.is_empty() {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                count = expired_kc_ids.len(),
                "[DKG SYNC] Removing expired KCs from queue"
            );
            self.repository_manager
                .kc_sync_repository()
                .remove_kcs(blockchain_id.as_str(), &contract_addr_str, &expired_kc_ids)
                .await
                .map_err(|e| format!("Failed to remove expired KCs: {}", e))?;
        }

        // Remove already-synced KCs from queue
        if !already_synced_kc_ids.is_empty() {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                count = already_synced_kc_ids.len(),
                "[DKG SYNC] Removing already-synced KCs from queue"
            );
            self.repository_manager
                .kc_sync_repository()
                .remove_kcs(blockchain_id.as_str(), &contract_addr_str, &already_synced_kc_ids)
                .await
                .map_err(|e| format!("Failed to remove already-synced KCs: {}", e))?;
        }

        let synced = already_synced_kc_ids.len() as u64;

        if kcs_to_sync.is_empty() {
            return Ok(ContractSyncResult {
                enqueued,
                pending,
                synced,
                failed: 0,
            });
        }

        // TODO 5: For KCs not found locally and not expired, batch GET from network
        //         (in chunks of NETWORK_BATCH_SIZE)

        // TODO 6: Store fetched KCs in triple store

        // TODO 7: Update DB: mark successful KCs as synced, increment retry_count for failed ones

        Ok(ContractSyncResult {
            enqueued,
            pending,
            synced,
            failed: 0,
        })
    }

    /// Filter pending KCs: check which are expired on chain or already exist locally.
    /// Returns (kcs_to_sync, expired_kc_ids, already_synced_kc_ids)
    async fn filter_pending_kcs(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        contract_addr_str: &str,
        pending_kc_ids: &[u64],
    ) -> (Vec<KcToSync>, Vec<u64>, Vec<u64>) {
        let mut kcs_to_sync = Vec::new();
        let mut expired_kc_ids = Vec::new();
        let mut already_synced_kc_ids = Vec::new();

        // Get current epoch once for all KCs
        let current_epoch = match self.blockchain_manager.get_current_epoch(blockchain_id).await {
            Ok(epoch) => epoch,
            Err(e) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    error = %e,
                    "[DKG SYNC] Failed to get current epoch, skipping expiration checks"
                );
                // Can't check expiration without current epoch, process all KCs
                u64::MAX
            }
        };

        for &kc_id in pending_kc_ids {
            // Check if KC is expired (currentEpoch > endEpoch)
            let end_epoch = match self
                .blockchain_manager
                .get_kc_end_epoch(blockchain_id, contract_address, kc_id as u128)
                .await
            {
                Ok(epoch) => epoch,
                Err(e) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc_id,
                        error = %e,
                        "[DKG SYNC] Failed to get KC end epoch, will retry later"
                    );
                    continue;
                }
            };

            // KC is expired if current epoch > end epoch
            if current_epoch > end_epoch {
                expired_kc_ids.push(kc_id);
                continue;
            }

            // Get token ID range for the KC
            let token_range = match self
                .blockchain_manager
                .get_knowledge_assets_range(blockchain_id, contract_address, kc_id as u128)
                .await
            {
                Ok(Some(range)) => range,
                Ok(None) => {
                    // KC doesn't exist on chain (shouldn't happen if we got end_epoch)
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc_id,
                        "[DKG SYNC] KC has end_epoch but no token range, skipping"
                    );
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc_id,
                        error = %e,
                        "[DKG SYNC] Failed to get KC token range, will retry later"
                    );
                    continue;
                }
            };

            let (start_token_id, end_token_id, burned) = token_range;

            // Build UAL to check local existence
            let kc_ual = derive_ual(blockchain_id, &contract_address, kc_id as u128, None);

            // Check if KC already exists locally
            let exists_locally = self
                .triple_store_service
                .knowledge_collection_exists(&kc_ual, start_token_id, end_token_id)
                .await;

            if exists_locally {
                // KC already exists locally
                already_synced_kc_ids.push(kc_id);
                continue;
            }

            // KC needs to be synced
            kcs_to_sync.push(KcToSync {
                kc_id,
                ual: kc_ual,
                start_token_id,
                end_token_id,
                burned,
            });
        }

        (kcs_to_sync, expired_kc_ids, already_synced_kc_ids)
    }

    /// Check for new KCs on chain and enqueue any that need syncing.
    async fn enqueue_new_kcs(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        contract_addr_str: &str,
    ) -> Result<u64, String> {
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
            .get_progress(blockchain_id.as_str(), contract_addr_str)
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
            .enqueue_kcs(blockchain_id.as_str(), contract_addr_str, &new_kc_ids)
            .await
            .map_err(|e| format!("Failed to enqueue KCs: {}", e))?;

        // Update progress to the highest ID we've now checked
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

        // Sync each contract in parallel
        let sync_futures = contract_addresses.iter().map(|&contract_address| {
            self.sync_contract(&data.blockchain_id, contract_address)
        });

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
