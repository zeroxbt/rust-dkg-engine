use std::{collections::HashSet, time::Duration};

use dkg_blockchain::{
    Address, BlockchainId, ContractEvent, ContractName, decode_contract_event,
    monitored_contract_events,
};
use dkg_observability as observability;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::SyncConfig;
use crate::{periodic_tasks::SyncDeps, periodic_tasks::runner::run_with_shutdown};

pub(crate) struct MetadataSyncTask {
    config: SyncConfig,
    deps: SyncDeps,
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
}

#[derive(Default)]
struct ContractMetadataSyncResult {
    metadata_events_found: u64,
    cursor_advanced: bool,
}

impl MetadataSyncTask {
    pub(crate) fn new(deps: SyncDeps, config: SyncConfig) -> Self {
        Self { config, deps }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("sync_metadata", shutdown, || self.execute(blockchain_id)).await;
    }

    #[tracing::instrument(name = "periodic_tasks.sync_metadata", skip(self), fields(blockchain_id = %blockchain_id))]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let idle_period = Duration::from_secs(self.config.sync_idle_sleep_secs.max(1));
        if !self.config.enabled || !self.config.metadata_backfill_enabled {
            return idle_period;
        }

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

        let mut total_events = 0_u64;
        let mut any_cursor_advanced = false;

        for contract_address in contract_addresses {
            match self
                .sync_contract(blockchain_id, contract_address, target_tip)
                .await
            {
                Ok(result) => {
                    total_events = total_events.saturating_add(result.metadata_events_found);
                    any_cursor_advanced |= result.cursor_advanced;
                }
                Err(error) => {
                    tracing::error!(
                        blockchain_id = %blockchain_id,
                        contract = ?contract_address,
                        error = %error,
                        "Failed metadata sync for contract"
                    );
                }
            }
        }

        if any_cursor_advanced || total_events > 0 {
            return Duration::ZERO;
        }

        idle_period
    }

    async fn sync_contract(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        target_tip: u64,
    ) -> Result<ContractMetadataSyncResult, MetadataSyncError> {
        let contract_addr_str = format!("{:?}", contract_address);
        let mut result = ContractMetadataSyncResult::default();

        let mut cursor = self
            .deps
            .kc_sync_repository
            .get_metadata_progress(blockchain_id.as_str(), &contract_addr_str)
            .await
            .map_err(MetadataSyncError::LoadMetadataProgress)?;

        let from_block = if cursor == 0 {
            match self
                .deps
                .blockchain_manager
                .find_contract_deployment_block(blockchain_id, contract_address, target_tip)
                .await
                .map_err(MetadataSyncError::FindDeploymentBlock)?
            {
                Some(start) => start,
                None => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        "No deployment block found for KC storage contract; skipping metadata sync"
                    );
                    return Ok(result);
                }
            }
        } else {
            cursor.saturating_add(1)
        };

        if from_block > target_tip {
            observability::record_sync_metadata_cursor(
                blockchain_id.as_str(),
                &contract_addr_str,
                cursor,
            );
            observability::record_sync_metadata_cursor_lag(
                blockchain_id.as_str(),
                &contract_addr_str,
                target_tip.saturating_sub(cursor),
            );
            return Ok(result);
        }

        let event_signatures = monitored_contract_events()
            .get(&ContractName::KnowledgeCollectionStorage)
            .cloned()
            .unwrap_or_default();
        let chunk_size = self.config.metadata_backfill_block_batch_size.max(1);
        let mut chunk_from = from_block;

        while chunk_from <= target_tip {
            let chunk_to = target_tip.min(chunk_from.saturating_add(chunk_size.saturating_sub(1)));
            let chunk_started = std::time::Instant::now();

            let logs = self
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
                .map_err(MetadataSyncError::FetchMetadataChunk)?;

            let mut discovered_ids = HashSet::new();
            let mut chunk_events_found = 0_usize;

            for log in logs {
                let Some(ContractEvent::KnowledgeCollectionCreated {
                    event,
                    contract_address: event_contract_address,
                    transaction_hash,
                    block_number,
                    block_timestamp,
                }) = decode_contract_event(log.contract_name(), log.log())
                else {
                    continue;
                };

                let kc_id_u128: u128 = event.id.to();
                let Ok(kc_id) = u64::try_from(kc_id_u128) else {
                    continue;
                };
                let Some(tx_hash) = transaction_hash else {
                    continue;
                };

                let publisher_address = self
                    .resolve_kc_publisher(
                        blockchain_id,
                        event_contract_address,
                        kc_id_u128,
                        tx_hash,
                    )
                    .await;

                let tx_hash_str = format!("{:#x}", tx_hash);
                self.deps
                    .kc_chain_metadata_repository
                    .upsert_core_metadata(
                        blockchain_id.as_str(),
                        &contract_addr_str,
                        kc_id,
                        publisher_address.as_deref(),
                        Some(block_number),
                        Some(&tx_hash_str),
                        Some(block_timestamp),
                        Some(&event.publishOperationId),
                        Some("sync_metadata_backfill"),
                    )
                    .await
                    .map_err(MetadataSyncError::UpsertCoreMetadata)?;

                discovered_ids.insert(kc_id);
                result.metadata_events_found = result.metadata_events_found.saturating_add(1);
                chunk_events_found = chunk_events_found.saturating_add(1);
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

            self.deps
                .kc_sync_repository
                .upsert_metadata_progress(blockchain_id.as_str(), &contract_addr_str, chunk_to)
                .await
                .map_err(MetadataSyncError::UpdateMetadataProgress)?;

            cursor = chunk_to;
            result.cursor_advanced = true;

            observability::record_sync_metadata_backfill_chunk(
                blockchain_id.as_str(),
                "ok",
                chunk_started.elapsed(),
                chunk_to.saturating_sub(chunk_from).saturating_add(1),
                chunk_events_found,
            );

            chunk_from = chunk_to.saturating_add(1);
        }

        observability::record_sync_metadata_events_found_total(
            blockchain_id.as_str(),
            result.metadata_events_found,
        );
        observability::record_sync_metadata_cursor(
            blockchain_id.as_str(),
            &contract_addr_str,
            cursor,
        );
        observability::record_sync_metadata_cursor_lag(
            blockchain_id.as_str(),
            &contract_addr_str,
            target_tip.saturating_sub(cursor),
        );

        Ok(result)
    }

    async fn resolve_kc_publisher(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        kc_id: u128,
        tx_hash: dkg_blockchain::B256,
    ) -> Option<String> {
        match self
            .deps
            .blockchain_manager
            .get_knowledge_collection_publisher(blockchain_id, contract_address, kc_id)
            .await
        {
            Ok(Some(address)) => Some(format!("{:?}", address)),
            _ => match self
                .deps
                .blockchain_manager
                .get_transaction_sender(blockchain_id, tx_hash)
                .await
            {
                Ok(Some(address)) => Some(format!("{:?}", address)),
                _ => None,
            },
        }
    }
}
