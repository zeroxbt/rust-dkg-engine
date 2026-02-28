use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant},
};

use dkg_blockchain::{
    Address, BlockchainId, ContractEvent, ContractName, decode_contract_event,
    monitored_contract_events,
};
use dkg_observability as observability;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::SyncConfig;
use crate::{
    application::kc_chain_metadata_sync::{
        BuildKcRecordError, build_kc_chain_metadata_record, hydrate_block_timestamps,
        hydrate_core_metadata_publishers, hydrate_kc_state_metadata,
        upsert_kc_chain_metadata_record,
    },
    periodic_tasks::SyncDeps,
    periodic_tasks::runner::run_with_shutdown,
};

pub(crate) struct MetadataSyncTask {
    config: SyncConfig,
    deps: SyncDeps,
    active_range_by_contract: Mutex<HashMap<String, BlockRange>>,
    deployment_block_by_contract: Mutex<HashMap<String, Option<u64>>>,
}

#[derive(Debug, Error)]
enum MetadataSyncError {
    #[error("Failed to load metadata sync progress")]
    LoadMetadataProgress(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to update metadata sync progress")]
    UpdateMetadataProgress(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to fetch metadata events chunk")]
    FetchMetadataChunk(#[source] dkg_blockchain::BlockchainError),
    #[error("Failed to fetch deployment block")]
    FindDeploymentBlock(#[source] dkg_blockchain::BlockchainError),
    #[error("Failed to upsert core metadata")]
    UpsertCoreMetadata(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to enqueue KC IDs")]
    EnqueueKcs(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to query oldest KC gap range")]
    FindOldestGapRange(#[source] dkg_repository::error::RepositoryError),
    #[error(
        "Incomplete metadata hydration for chunk [{chunk_from}..{chunk_to}] in contract {contract}; {dropped_not_ready} records missing full metadata"
    )]
    IncompleteChunkHydration {
        contract: String,
        chunk_from: u64,
        chunk_to: u64,
        dropped_not_ready: usize,
    },
}

#[derive(Default)]
struct ContractMetadataSyncResult {
    metadata_events_found: u64,
    cursor_advanced: bool,
    gap_ranges_detected: u64,
    chunk_processed: bool,
}

/// An inclusive block range [start, end] to scan for KC events.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockRange {
    start: u64,
    end: u64,
}

impl MetadataSyncTask {
    pub(crate) fn new(deps: SyncDeps, config: SyncConfig) -> Self {
        Self {
            config,
            deps,
            active_range_by_contract: Mutex::new(HashMap::new()),
            deployment_block_by_contract: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("sync_metadata", shutdown, || self.execute(blockchain_id)).await;
    }

    #[tracing::instrument(name = "periodic_tasks.sync_metadata", skip(self), fields(blockchain_id = %blockchain_id))]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let idle_period = Duration::from_secs(self.config.sync_idle_sleep_secs.max(1));
        let hot_loop_period = Duration::from_millis(500);
        let recheck_period =
            Duration::from_secs(self.config.metadata_gap_recheck_interval_secs.max(1));

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
                    "Failed to resolve current block for metadata sync cycle"
                );
                return idle_period;
            }
        };
        let target_tip = current_block.saturating_sub(self.config.head_safety_blocks);

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
                    "Failed to get KC storage contract addresses for metadata sync"
                );
                return idle_period;
            }
        };

        // Process one chunk from the oldest unresolved range per contract per execution.
        let results =
            futures::future::join_all(contract_addresses.iter().map(|&contract_address| {
                self.sync_contract(blockchain_id, contract_address, target_tip, true)
            }))
            .await;

        let mut any_chunk_processed = false;
        for (index, result) in results.into_iter().enumerate() {
            match result {
                Ok(contract_result) => {
                    any_chunk_processed |= contract_result.chunk_processed;
                }
                Err(error) => {
                    tracing::error!(
                        blockchain_id = %blockchain_id,
                        contract = ?contract_addresses[index],
                        error = %error,
                        "Failed metadata sync for contract"
                    );
                }
            }
        }

        if any_chunk_processed {
            return hot_loop_period;
        }

        recheck_period
    }

    async fn sync_contract(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        target_tip: u64,
        single_chunk: bool,
    ) -> Result<ContractMetadataSyncResult, MetadataSyncError> {
        let contract_addr_str = format!("{:?}", contract_address);
        let mut result = ContractMetadataSyncResult::default();

        let cursor = self
            .deps
            .kc_sync_repository
            .get_metadata_progress(blockchain_id.as_str(), &contract_addr_str)
            .await
            .map_err(MetadataSyncError::LoadMetadataProgress)?;

        // Fast path: keep processing cached active range for this contract until exhausted.
        let mut active_range = self
            .cached_active_range(&contract_addr_str)
            .and_then(|range| {
                let start = range.start.max(cursor.saturating_add(1));
                if start <= range.end {
                    Some(BlockRange {
                        start,
                        end: range.end,
                    })
                } else {
                    None
                }
            });
        if active_range.is_none() {
            self.clear_cached_active_range(&contract_addr_str);
        }

        if active_range.is_none() {
            let deployment_block = self
                .resolve_deployment_block(
                    blockchain_id,
                    contract_address,
                    &contract_addr_str,
                    target_tip,
                )
                .await?;

            // Query only one gap range when we need to pick a new active range.
            let oldest_gap = self
                .deps
                .kc_chain_metadata_repository
                .find_oldest_gap_range(
                    blockchain_id.as_str(),
                    &contract_addr_str,
                    cursor,
                    target_tip,
                    deployment_block,
                )
                .await
                .map_err(MetadataSyncError::FindOldestGapRange)?;

            result.gap_ranges_detected = oldest_gap.as_ref().map(|_| 1).unwrap_or(0);

            active_range = oldest_gap.map(|gap| BlockRange {
                start: gap.start_block.max(cursor.saturating_add(1)),
                end: gap.end_block,
            });

            let Some(range) = active_range
                .as_ref()
                .filter(|range| range.start <= range.end)
            else {
                self.clear_cached_active_range(&contract_addr_str);
                return Ok(result);
            };
            self.set_cached_active_range(&contract_addr_str, range);
        }

        let active_range =
            active_range.expect("active_range should be present after cache/query selection");

        let event_signatures = monitored_contract_events()
            .get(&ContractName::KnowledgeCollectionStorage)
            .cloned()
            .unwrap_or_default();
        let chunk_size = self.config.metadata_backfill_block_batch_size.max(1);

        let mut chunk_from = active_range.start;

        while chunk_from <= active_range.end {
            let chunk_to = active_range
                .end
                .min(chunk_from.saturating_add(chunk_size.saturating_sub(1)));
            let chunk_blocks_scanned = chunk_to.saturating_sub(chunk_from).saturating_add(1);

            let fetch_started = Instant::now();
            let logs = match self
                .deps
                .blockchain_manager
                .get_event_logs_for_address(
                    blockchain_id,
                    ContractName::KnowledgeCollectionStorage,
                    contract_address,
                    &event_signatures,
                    chunk_from,
                    chunk_to,
                )
                .await
            {
                Ok(logs) => {
                    observability::record_sync_metadata_backfill_batch_fetch_duration(
                        blockchain_id.as_str(),
                        "success",
                        fetch_started.elapsed(),
                    );
                    logs
                }
                Err(error) => {
                    observability::record_sync_metadata_backfill_batch_fetch_duration(
                        blockchain_id.as_str(),
                        "error",
                        fetch_started.elapsed(),
                    );
                    return Err(MetadataSyncError::FetchMetadataChunk(error));
                }
            };
            let processing_started = Instant::now();

            let mut records = Vec::new();
            let mut discovered_ids = HashSet::new();
            let mut chunk_events_found = 0_usize;

            for log in logs {
                let Some(ContractEvent::KnowledgeCollectionCreated {
                    event,
                    transaction_hash,
                    block_number,
                    block_timestamp,
                    ..
                }) = decode_contract_event(log.contract_name(), log.log())
                else {
                    continue;
                };

                let record = match build_kc_chain_metadata_record(
                    event.publishOperationId.clone(),
                    event.id,
                    event.merkleRoot,
                    event.byteSize.to(),
                    contract_address,
                    transaction_hash,
                    block_number,
                    block_timestamp,
                ) {
                    Ok(record) => record,
                    Err(BuildKcRecordError::KcIdOutOfRange) => continue,
                    Err(BuildKcRecordError::MissingTransactionHash) => continue,
                };

                discovered_ids.insert(record.kc_id);
                records.push(record);
                chunk_events_found = chunk_events_found.saturating_add(1);
                result.metadata_events_found = result.metadata_events_found.saturating_add(1);
            }

            hydrate_block_timestamps(
                self.deps.blockchain_manager.as_ref(),
                blockchain_id,
                &mut records,
            )
            .await;
            hydrate_core_metadata_publishers(
                self.deps.blockchain_manager.as_ref(),
                blockchain_id,
                &mut records,
            )
            .await;
            hydrate_kc_state_metadata(
                self.deps.blockchain_manager.as_ref(),
                blockchain_id,
                &mut records,
                self.config.metadata_state_batch_size.max(1),
            )
            .await;

            let before_ready_filter = records.len();
            records.retain(|record| {
                record.block_timestamp > 0
                    && record.publisher_address.is_some()
                    && record.kc_state_metadata.is_some()
            });
            let dropped_not_ready = before_ready_filter.saturating_sub(records.len());
            if dropped_not_ready > 0 {
                observability::record_sync_metadata_backfill_batch(
                    blockchain_id.as_str(),
                    "error",
                    chunk_blocks_scanned,
                    chunk_events_found,
                );
                observability::record_sync_metadata_backfill_batch_processing_duration(
                    blockchain_id.as_str(),
                    "error",
                    processing_started.elapsed(),
                );
                return Err(MetadataSyncError::IncompleteChunkHydration {
                    contract: contract_addr_str.clone(),
                    chunk_from,
                    chunk_to,
                    dropped_not_ready,
                });
            }

            if !records.is_empty() {
                tracing::trace!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    hydrated_records = records.len(),
                    "Chunk metadata hydration complete; persisting records"
                );
            }

            for record in &records {
                upsert_kc_chain_metadata_record(
                    &self.deps.kc_chain_metadata_repository,
                    blockchain_id.as_str(),
                    "sync_metadata_backfill",
                    record,
                )
                .await
                .map_err(MetadataSyncError::UpsertCoreMetadata)?;
            }

            if !discovered_ids.is_empty() {
                let mut ids: Vec<u64> = discovered_ids.into_iter().collect();
                ids.sort_unstable();
                self.deps
                    .kc_sync_repository
                    .enqueue_kcs(blockchain_id.as_str(), &contract_addr_str, &ids)
                    .await
                    .map_err(MetadataSyncError::EnqueueKcs)?;
            }

            // Cursor always advances inside the chosen active range.
            self.deps
                .kc_sync_repository
                .upsert_metadata_progress(blockchain_id.as_str(), &contract_addr_str, chunk_to)
                .await
                .map_err(MetadataSyncError::UpdateMetadataProgress)?;
            result.cursor_advanced = true;
            if chunk_to >= active_range.end {
                self.clear_cached_active_range(&contract_addr_str);
            }

            observability::record_sync_metadata_backfill_batch(
                blockchain_id.as_str(),
                "success",
                chunk_blocks_scanned,
                chunk_events_found,
            );
            observability::record_sync_metadata_backfill_batch_processing_duration(
                blockchain_id.as_str(),
                "success",
                processing_started.elapsed(),
            );
            result.chunk_processed = true;
            if single_chunk {
                return Ok(result);
            }

            chunk_from = chunk_to.saturating_add(1);
        }

        Ok(result)
    }

    fn cached_active_range(&self, contract_address: &str) -> Option<BlockRange> {
        self.active_range_by_contract
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(contract_address)
            .cloned()
    }

    fn set_cached_active_range(&self, contract_address: &str, range: &BlockRange) {
        self.active_range_by_contract
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(contract_address.to_string(), range.clone());
    }

    fn clear_cached_active_range(&self, contract_address: &str) {
        self.active_range_by_contract
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(contract_address);
    }

    async fn resolve_deployment_block(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        contract_address_str: &str,
        target_tip: u64,
    ) -> Result<Option<u64>, MetadataSyncError> {
        if let Some(cached) = self
            .deployment_block_by_contract
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(contract_address_str)
            .cloned()
        {
            return Ok(cached);
        }

        let resolved = self
            .deps
            .blockchain_manager
            .find_contract_deployment_block(blockchain_id, contract_address, target_tip)
            .await
            .map_err(MetadataSyncError::FindDeploymentBlock)?;

        self.deployment_block_by_contract
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(contract_address_str.to_string(), resolved);

        Ok(resolved)
    }
}
