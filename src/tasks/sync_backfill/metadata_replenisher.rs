use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::Instant,
};

use dkg_blockchain::{
    Address, BlockchainId, ContractEvent, ContractName, decode_contract_event,
    monitored_contract_events,
};
use dkg_observability as observability;
use dkg_repository::{SyncMetadataRecordInput, SyncMetadataStateInput};
use thiserror::Error;

use super::SyncConfig;
use crate::{
    application::kc_chain_metadata_sync::{
        BuildKcRecordError, build_kc_chain_metadata_record, hydrate_block_timestamps,
        hydrate_core_metadata_publishers, hydrate_kc_state_metadata,
    },
    tasks::periodic::SyncDeps,
};

pub(crate) struct MetadataReplenisher {
    config: SyncConfig,
    deps: SyncDeps,
    active_range_by_contract: Mutex<HashMap<String, BlockRange>>,
    deployment_block_by_contract: Mutex<HashMap<String, Option<u64>>>,
}

#[derive(Debug, Error)]
pub(crate) enum MetadataReplenisherError {
    #[error("Failed to load metadata sync progress")]
    LoadMetadataProgress(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to fetch metadata events chunk")]
    FetchMetadataChunk(#[source] dkg_blockchain::BlockchainError),
    #[error("Failed to fetch deployment block")]
    FindDeploymentBlock(#[source] dkg_blockchain::BlockchainError),
    #[error("Failed to upsert core metadata")]
    UpsertCoreMetadata(#[source] dkg_repository::error::RepositoryError),
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
pub(crate) struct ReplenishContractOutcome {
    pub(crate) chunk_processed: bool,
}

/// An inclusive block range [start, end] to scan for KC events.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockRange {
    start: u64,
    end: u64,
}

impl MetadataReplenisher {
    pub(crate) fn new(deps: SyncDeps, config: SyncConfig) -> Self {
        Self {
            config,
            deps,
            active_range_by_contract: Mutex::new(HashMap::new()),
            deployment_block_by_contract: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn replenish_contract_once(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        target_tip: u64,
    ) -> Result<ReplenishContractOutcome, MetadataReplenisherError> {
        let contract_addr_str = format!("{:?}", contract_address);
        let mut result = ReplenishContractOutcome::default();

        let cursor = self
            .deps
            .kc_sync_repository
            .get_metadata_progress(blockchain_id.as_str(), &contract_addr_str)
            .await
            .map_err(MetadataReplenisherError::LoadMetadataProgress)?;

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
                .map_err(MetadataReplenisherError::FindOldestGapRange)?;

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

        let chunk_from = active_range.start;
        if chunk_from <= active_range.end {
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
                    return Err(MetadataReplenisherError::FetchMetadataChunk(error));
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
                return Err(MetadataReplenisherError::IncompleteChunkHydration {
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

            let metadata_inputs: Vec<SyncMetadataRecordInput> = records
                .iter()
                .map(|record| SyncMetadataRecordInput {
                    kc_id: record.kc_id,
                    publisher_address: record.publisher_address.clone(),
                    block_number: record.block_number,
                    transaction_hash: format!("{:#x}", record.transaction_hash),
                    block_timestamp: record.block_timestamp,
                    publish_operation_id: record.publish_operation_id.clone(),
                    source: "sync_metadata_backfill".to_string(),
                    state: record
                        .kc_state_metadata
                        .as_ref()
                        .map(|state| SyncMetadataStateInput {
                            range_start_token_id: state.range_start_token_id,
                            range_end_token_id: state.range_end_token_id,
                            burned_mode: state.burned_mode,
                            burned_payload: state.burned_payload.clone(),
                            end_epoch: state.end_epoch,
                            latest_merkle_root: state.latest_merkle_root.clone(),
                        }),
                })
                .collect();
            let mut ids: Vec<u64> = discovered_ids.into_iter().collect();
            ids.sort_unstable();

            self.deps
                .kc_sync_repository
                .persist_metadata_chunk_and_enqueue(
                    blockchain_id.as_str(),
                    &contract_addr_str,
                    chunk_to,
                    &metadata_inputs,
                    &ids,
                )
                .await
                .map_err(MetadataReplenisherError::UpsertCoreMetadata)?;
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
            return Ok(result);
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
    ) -> Result<Option<u64>, MetadataReplenisherError> {
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
            .map_err(MetadataReplenisherError::FindDeploymentBlock)?;

        self.deployment_block_by_contract
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(contract_address_str.to_string(), resolved);

        Ok(resolved)
    }
}
