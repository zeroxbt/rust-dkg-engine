use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::Instant,
};

use dkg_blockchain::{
    Address, BlockchainId, ContractName, KcStorageEvent, decode_kc_storage_event,
    kc_storage_event_signatures,
};
use dkg_domain::{KnowledgeCollectionMetadata, canonical_evm_address};
use dkg_observability as observability;
use dkg_repository::{SyncMetadataRecordInput, SyncMetadataStateInput};
use thiserror::Error;
use uuid::Uuid;

use super::DkgSyncConfig;
use crate::{
    application::kc_chain_metadata_sync::{
        BuildKcRecordError, build_kc_chain_metadata_record, hydrate_block_timestamps,
        hydrate_core_metadata_publishers, hydrate_kc_state_metadata,
    },
    commands::{
        executor::CommandExecutionRequest,
        operations::publish::finality::send_publish_finality_request::SendPublishFinalityRequestCommandData,
        registry::Command,
    },
    tasks::dkg_sync::DkgSyncDeps,
};

#[path = "loop.rs"]
pub(crate) mod discovery_loop;

pub(crate) struct DiscoveryWorker {
    config: DkgSyncConfig,
    deps: DkgSyncDeps,
    live_cursor_by_contract: Mutex<HashMap<String, u64>>,
    active_range_by_contract: Mutex<HashMap<String, BlockRange>>,
    deployment_block_by_contract: Mutex<HashMap<String, Option<u64>>>,
}

#[derive(Debug, Error)]
pub(crate) enum DiscoveryError {
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
pub(crate) struct DiscoverContractOutcome {
    pub(crate) chunk_processed: bool,
}

/// An inclusive block range [start, end] to scan for KC events.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockRange {
    start: u64,
    end: u64,
    last_expected_kc_id: Option<u64>,
}

impl DiscoveryWorker {
    pub(crate) fn new(deps: DkgSyncDeps, config: DkgSyncConfig) -> Self {
        Self {
            config,
            deps,
            live_cursor_by_contract: Mutex::new(HashMap::new()),
            active_range_by_contract: Mutex::new(HashMap::new()),
            deployment_block_by_contract: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn discover_live_once(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        pinned_tip: u64,
        target_tip: u64,
    ) -> Result<DiscoverContractOutcome, DiscoveryError> {
        let contract_addr_str = canonical_evm_address(&contract_address);
        let cursor = self.cached_live_cursor(&contract_addr_str, pinned_tip);

        let live_start = cursor.saturating_add(1);
        if live_start > target_tip {
            return Ok(DiscoverContractOutcome::default());
        }

        let chunk_to = Self::chunk_end(
            live_start,
            target_tip,
            self.config
                .discovery
                .metadata_discovery_max_blocks_per_chunk
                .max(1),
        );

        let outcome = self
            .process_range_chunk(
                blockchain_id,
                contract_address,
                &contract_addr_str,
                BlockRange {
                    start: live_start,
                    end: chunk_to,
                    last_expected_kc_id: None,
                },
                false,
                false,
            )
            .await?;

        if outcome.chunk_processed {
            self.set_live_cursor(&contract_addr_str, chunk_to);
        }

        Ok(outcome)
    }

    pub(crate) async fn discover_contract_once(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        target_tip: u64,
        pinned_latest_kc_id: u64,
    ) -> Result<DiscoverContractOutcome, DiscoveryError> {
        let contract_addr_str = canonical_evm_address(&contract_address);

        let cursor = self
            .deps
            .kc_sync_repository
            .get_metadata_progress(blockchain_id.as_str(), &contract_addr_str)
            .await
            .map_err(DiscoveryError::LoadMetadataProgress)?;

        // Fast path: keep processing cached active range for this contract until exhausted.
        let mut active_range = self
            .cached_active_range(&contract_addr_str)
            .and_then(|range| {
                let start = range.start.max(cursor.saturating_add(1));
                if start <= range.end {
                    Some(BlockRange {
                        start,
                        end: range.end,
                        last_expected_kc_id: range.last_expected_kc_id,
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
                    pinned_latest_kc_id,
                    target_tip,
                    deployment_block,
                )
                .await
                .map_err(DiscoveryError::FindOldestGapRange)?;

            active_range = oldest_gap.map(|gap| BlockRange {
                start: gap.start_block.max(cursor.saturating_add(1)),
                end: gap.end_block,
                last_expected_kc_id: gap.last_expected_kc_id,
            });

            let Some(range) = active_range
                .as_ref()
                .filter(|range| range.start <= range.end)
            else {
                self.clear_cached_active_range(&contract_addr_str);
                return Ok(DiscoverContractOutcome::default());
            };
            self.set_cached_active_range(&contract_addr_str, range);
        }

        let active_range =
            active_range.expect("active_range should be present after cache/query selection");

        self.process_range_chunk(
            blockchain_id,
            contract_address,
            &contract_addr_str,
            active_range,
            true,
            true,
        )
        .await
    }

    async fn process_range_chunk(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        contract_addr_str: &str,
        active_range: BlockRange,
        clear_cached_when_exhausted: bool,
        persist_cursor: bool,
    ) -> Result<DiscoverContractOutcome, DiscoveryError> {
        let mut result = DiscoverContractOutcome::default();
        let event_signatures = kc_storage_event_signatures();
        let chunk_size = self
            .config
            .discovery
            .metadata_discovery_max_blocks_per_chunk
            .max(1);

        let chunk_from = active_range.start;
        if chunk_from > active_range.end {
            return Ok(result);
        }

        let chunk_to = Self::chunk_end(chunk_from, active_range.end, chunk_size);
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
                observability::record_sync_metadata_discovery_batch_fetch_duration(
                    blockchain_id.as_str(),
                    "success",
                    fetch_started.elapsed(),
                );
                logs
            }
            Err(error) => {
                observability::record_sync_metadata_discovery_batch_fetch_duration(
                    blockchain_id.as_str(),
                    "error",
                    fetch_started.elapsed(),
                );
                return Err(DiscoveryError::FetchMetadataChunk(error));
            }
        };
        let processing_started = Instant::now();

        let mut records = Vec::new();
        let mut discovered_ids = HashSet::new();
        let mut chunk_events_found = 0_usize;

        for log in logs {
            let Some(KcStorageEvent::KnowledgeCollectionCreated {
                event,
                transaction_hash,
                block_number,
                block_timestamp,
                ..
            }) = decode_kc_storage_event(log.log())
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
        let chunk_max_discovered_kc_id = discovered_ids.iter().copied().max();

        hydrate_block_timestamps(
            self.deps.blockchain_manager.as_ref(),
            blockchain_id,
            &mut records,
        )
        .await;
        hydrate_kc_state_metadata(
            self.deps.blockchain_manager.as_ref(),
            blockchain_id,
            &mut records,
            self.config.discovery.metadata_state_max_kc_per_chunk.max(1),
        )
        .await;
        hydrate_core_metadata_publishers(
            self.deps.blockchain_manager.as_ref(),
            blockchain_id,
            &mut records,
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
            observability::record_sync_metadata_discovery_batch(
                blockchain_id.as_str(),
                "error",
                chunk_blocks_scanned,
                chunk_events_found,
            );
            observability::record_sync_metadata_discovery_batch_processing_duration(
                blockchain_id.as_str(),
                "error",
                processing_started.elapsed(),
            );
            return Err(DiscoveryError::IncompleteChunkHydration {
                contract: contract_addr_str.to_string(),
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
                publisher_address: record
                    .publisher_address
                    .clone()
                    .expect("publisher address checked in ready filter"),
                block_number: record.block_number,
                transaction_hash: format!("{:#x}", record.transaction_hash),
                block_timestamp: record.block_timestamp,
                publish_operation_id: record.publish_operation_id.clone(),
                source: "dkg_sync".to_string(),
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

        let mut enqueue_ids = Vec::new();
        let mut finality_requests = Vec::new();
        let mut scheduled_finality_kc_ids = HashSet::new();

        for record in &records {
            let publish_operation_id = match Uuid::parse_str(&record.publish_operation_id) {
                Ok(value) => value,
                Err(error) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = record.kc_id,
                        publish_operation_id = %record.publish_operation_id,
                        error = %error,
                        "Invalid publish operation id in metadata chunk; falling back to sync queue"
                    );
                    enqueue_ids.push(record.kc_id);
                    continue;
                }
            };

            let pending_dataset = match self
                .deps
                .publish_tmp_dataset_store
                .get(publish_operation_id)
                .await
            {
                Ok(Some(data)) => data,
                Ok(None) => {
                    enqueue_ids.push(record.kc_id);
                    continue;
                }
                Err(error) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = record.kc_id,
                        publish_operation_id = %record.publish_operation_id,
                        error = %error,
                        "Failed to read publish tmp dataset for metadata chunk; falling back to sync queue"
                    );
                    enqueue_ids.push(record.kc_id);
                    continue;
                }
            };

            if !scheduled_finality_kc_ids.insert(record.kc_id) {
                continue;
            }

            let metadata = KnowledgeCollectionMetadata::new(
                record
                    .publisher_address
                    .clone()
                    .expect("publisher address checked in ready filter"),
                record.block_number,
                format!("{:#x}", record.transaction_hash),
                record.block_timestamp,
            );
            let (dataset, publisher_peer_id) = pending_dataset.into_parts();
            finality_requests.push(SendPublishFinalityRequestCommandData::new(
                blockchain_id.to_owned(),
                record.publish_operation_id.clone(),
                dkg_blockchain::U256::from(record.kc_id),
                record.contract_address,
                record.byte_size,
                record.merkle_root,
                dataset,
                publisher_peer_id,
                metadata,
            ));
        }
        enqueue_ids.retain(|kc_id| !scheduled_finality_kc_ids.contains(kc_id));
        enqueue_ids.sort_unstable();
        enqueue_ids.dedup();

        let reached_last_expected_kc = active_range
            .last_expected_kc_id
            .zip(chunk_max_discovered_kc_id)
            .is_some_and(|(last_expected, discovered_max)| discovered_max >= last_expected);
        if reached_last_expected_kc {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                chunk_from,
                chunk_to,
                last_expected_kc_id = active_range.last_expected_kc_id,
                discovered_max_kc_id = chunk_max_discovered_kc_id,
                "Gap discovery reached last expected KC id; fast-forwarding cursor to gap end"
            );
        }
        let cursor_to_persist = if reached_last_expected_kc {
            active_range.end
        } else {
            chunk_to
        };

        if let Err(error) = self
            .deps
            .kc_sync_repository
            .persist_metadata_chunk_and_enqueue(
                blockchain_id.as_str(),
                contract_addr_str,
                cursor_to_persist,
                &metadata_inputs,
                &enqueue_ids,
                persist_cursor,
            )
            .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                chunk_from,
                chunk_to,
                records = metadata_inputs.len(),
                discovered_ids = ids.len(),
                enqueue_ids = enqueue_ids.len(),
                finality_requests = finality_requests.len(),
                error = ?error,
                "Failed to persist metadata chunk"
            );
            return Err(DiscoveryError::UpsertCoreMetadata(error));
        }

        if let Err(error) = self
            .deps
            .kc_projection_repository
            .ensure_desired_present(blockchain_id.as_str(), contract_addr_str, &ids)
            .await
        {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                chunk_from,
                chunk_to,
                discovered_ids = ids.len(),
                error = %error,
                "Failed to upsert projection desired state for metadata chunk"
            );
        }

        for request in finality_requests {
            self.deps
                .command_scheduler
                .schedule(CommandExecutionRequest::new(
                    Command::SendPublishFinalityRequest(request),
                ))
                .await;
        }

        if clear_cached_when_exhausted && cursor_to_persist >= active_range.end {
            self.clear_cached_active_range(contract_addr_str);
        }

        observability::record_sync_metadata_discovery_batch(
            blockchain_id.as_str(),
            "success",
            chunk_blocks_scanned,
            chunk_events_found,
        );
        observability::record_sync_metadata_discovery_batch_processing_duration(
            blockchain_id.as_str(),
            "success",
            processing_started.elapsed(),
        );
        result.chunk_processed = true;
        Ok(result)
    }

    fn chunk_end(start: u64, end: u64, chunk_size: u64) -> u64 {
        end.min(start.saturating_add(chunk_size.saturating_sub(1)))
    }

    fn cached_live_cursor(&self, contract_address: &str, pinned_tip: u64) -> u64 {
        let mut cursors = self
            .live_cursor_by_contract
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *cursors
            .entry(contract_address.to_string())
            .or_insert(pinned_tip)
    }

    fn set_live_cursor(&self, contract_address: &str, cursor: u64) {
        let mut cursors = self
            .live_cursor_by_contract
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = cursors
            .entry(contract_address.to_string())
            .or_insert(cursor);
        *entry = (*entry).max(cursor);
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
    ) -> Result<Option<u64>, DiscoveryError> {
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
            .map_err(DiscoveryError::FindDeploymentBlock)?;

        self.deployment_block_by_contract
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(contract_address_str.to_string(), resolved);

        Ok(resolved)
    }
}
