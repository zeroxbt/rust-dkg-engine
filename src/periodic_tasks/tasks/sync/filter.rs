//! Filter stage: checks local existence and SQL readiness, then filters expired KCs.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use dkg_blockchain::{Address, BlockchainId, BlockchainManager};
use dkg_domain::{KnowledgeCollectionMetadata, TokenIds, derive_ual};
use dkg_observability as observability;
use dkg_repository::{KcChainMetadataRepository, KcChainReadyForSyncEntry};
use tokio::sync::mpsc;
use tracing::instrument;

use super::{
    types::{FilterStats, KcToSync},
};
use crate::application::TripleStoreAssertions;
use crate::application::state_metadata::{BurnedMode, decode_burned_ids};

#[allow(clippy::too_many_arguments)]
#[instrument(
    name = "sync_filter",
    skip(
        pending_kc_ids,
        blockchain_manager,
        kc_chain_metadata_repository,
        triple_store_assertions,
        tx
    ),
    fields(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
        pending_count = pending_kc_ids.len(),
    )
)]
pub(crate) async fn filter_task(
    pending_kc_ids: Vec<u64>,
    filter_batch_size: usize,
    blockchain_id: BlockchainId,
    contract_address: Address,
    contract_addr_str: String,
    blockchain_manager: Arc<BlockchainManager>,
    kc_chain_metadata_repository: KcChainMetadataRepository,
    triple_store_assertions: Arc<TripleStoreAssertions>,
    tx: mpsc::Sender<Vec<KcToSync>>,
) -> FilterStats {
    let task_start = Instant::now();
    let blockchain_label = blockchain_id.as_str();
    let mut already_synced = Vec::new();
    let mut expired = Vec::new();
    let mut waiting_for_metadata = Vec::new();
    let mut waiting_for_state = Vec::new();
    let total_kcs = pending_kc_ids.len();
    let mut processed = 0usize;

    let current_epoch = blockchain_manager
        .get_current_epoch(&blockchain_id)
        .await
        .ok();
    let filter_batch_size = filter_batch_size.max(1);

    for chunk in pending_kc_ids.chunks(filter_batch_size) {
        let batch_start = Instant::now();
        let batch_result = process_filter_batch(
            chunk,
            &blockchain_id,
            &contract_address,
            &contract_addr_str,
            current_epoch,
            &kc_chain_metadata_repository,
            &triple_store_assertions,
        )
        .await;

        observability::record_sync_filter_batch(
            batch_start.elapsed(),
            chunk.len(),
            batch_result.to_sync.len(),
            batch_result.already_synced.len(),
            batch_result.expired.len(),
        );

        already_synced.extend(batch_result.already_synced);
        expired.extend(batch_result.expired);
        waiting_for_metadata.extend(batch_result.waiting_for_metadata);
        waiting_for_state.extend(batch_result.waiting_for_state);
        processed += chunk.len();

        if !batch_result.to_sync.is_empty() {
            tracing::trace!(
                batch_size = batch_result.to_sync.len(),
                processed,
                total_kcs,
                elapsed_ms = task_start.elapsed().as_millis() as u64,
                "Filter: sending batch to fetch stage"
            );

            if tx.send(batch_result.to_sync).await.is_err() {
                tracing::trace!("Filter: fetch stage receiver dropped, stopping");
                break;
            }
            observability::record_sync_pipeline_channel_depth(
                blockchain_label,
                "filter_to_fetch",
                tx.max_capacity().saturating_sub(tx.capacity()),
            );
        }
    }

    observability::record_sync_waiting_for_core_metadata_count(
        blockchain_label,
        waiting_for_metadata.len(),
    );
    observability::record_sync_waiting_for_state_count(blockchain_label, waiting_for_state.len());

    tracing::trace!(
        total_ms = task_start.elapsed().as_millis() as u64,
        already_synced = already_synced.len(),
        expired = expired.len(),
        waiting_for_metadata = waiting_for_metadata.len(),
        waiting_for_state = waiting_for_state.len(),
        "Filter task completed"
    );

    FilterStats {
        already_synced,
        expired,
        waiting_for_metadata,
        waiting_for_state,
    }
}

struct FilterBatchResult {
    already_synced: Vec<u64>,
    expired: Vec<u64>,
    waiting_for_metadata: Vec<u64>,
    waiting_for_state: Vec<u64>,
    to_sync: Vec<KcToSync>,
}

#[instrument(
    name = "filter_batch",
    skip(
        chunk,
        blockchain_id,
        contract_address,
        kc_chain_metadata_repository,
        triple_store_assertions
    ),
    fields(chunk_size = chunk.len())
)]
async fn process_filter_batch(
    chunk: &[u64],
    blockchain_id: &BlockchainId,
    contract_address: &Address,
    contract_addr_str: &str,
    current_epoch: Option<u64>,
    kc_chain_metadata_repository: &KcChainMetadataRepository,
    triple_store_assertions: &TripleStoreAssertions,
) -> FilterBatchResult {
    let mut already_synced = Vec::new();
    let mut expired = Vec::new();

    let kcs_needing_sync = check_local_existence(
        chunk,
        blockchain_id,
        contract_address,
        triple_store_assertions,
    )
    .await;

    let needing_sync_ids: HashSet<u64> = kcs_needing_sync.iter().map(|(id, _)| *id).collect();
    for &kc_id in chunk {
        if !needing_sync_ids.contains(&kc_id) {
            already_synced.push(kc_id);
        }
    }

    if kcs_needing_sync.is_empty() {
        return FilterBatchResult {
            already_synced,
            expired,
            waiting_for_metadata: Vec::new(),
            waiting_for_state: Vec::new(),
            to_sync: Vec::new(),
        };
    }

    let (ready, waiting_for_metadata, waiting_for_state) = gate_on_sql_readiness(
        &kcs_needing_sync,
        blockchain_id,
        contract_addr_str,
        kc_chain_metadata_repository,
    )
    .await;

    if ready.is_empty() {
        return FilterBatchResult {
            already_synced,
            expired,
            waiting_for_metadata,
            waiting_for_state,
            to_sync: Vec::new(),
        };
    }

    let mut to_sync = Vec::with_capacity(ready.len());
    for (kc_id, ual, entry) in ready {
        if let (Some(current), Some(end)) = (current_epoch, entry.end_epoch)
            && current > end
        {
            expired.push(kc_id);
            continue;
        }

        let Some(mode) = BurnedMode::from_raw(entry.burned_mode) else {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                kc_id,
                burned_mode = entry.burned_mode,
                "Invalid burned mode in SQL state"
            );
            continue;
        };
        let Some(burned) = decode_burned_ids(
            mode,
            entry.burned_payload.as_slice(),
            entry.range_start_token_id,
            entry.range_end_token_id,
        ) else {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                kc_id,
                "Failed to decode burned payload from SQL state"
            );
            continue;
        };

        let token_ids = TokenIds::new(entry.range_start_token_id, entry.range_end_token_id, burned);
        let metadata = KnowledgeCollectionMetadata::new(
            entry.publisher_address,
            entry.block_number,
            entry.transaction_hash,
            entry.block_timestamp,
        );
        to_sync.push(KcToSync {
            kc_id,
            ual,
            token_ids,
            merkle_root: Some(entry.latest_merkle_root),
            metadata,
        });
    }

    observability::record_sync_state_ready_count(blockchain_id.as_str(), to_sync.len());

    FilterBatchResult {
        already_synced,
        expired,
        waiting_for_metadata,
        waiting_for_state,
        to_sync,
    }
}

async fn gate_on_sql_readiness(
    kcs_needing_sync: &[(u64, String)],
    blockchain_id: &BlockchainId,
    contract_addr_str: &str,
    kc_chain_metadata_repository: &KcChainMetadataRepository,
) -> (
    Vec<(u64, String, KcChainReadyForSyncEntry)>,
    Vec<u64>,
    Vec<u64>,
) {
    if kcs_needing_sync.is_empty() {
        return (Vec::new(), Vec::new(), Vec::new());
    }

    let kc_ids: Vec<u64> = kcs_needing_sync.iter().map(|(kc_id, _)| *kc_id).collect();
    let ready_by_id = match kc_chain_metadata_repository
        .get_many_ready_for_sync(blockchain_id.as_str(), contract_addr_str, &kc_ids)
        .await
    {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %error,
                "Failed to query SQL ready rows"
            );
            HashMap::new()
        }
    };

    let ready_ids: Vec<u64> = ready_by_id.keys().copied().collect();
    let complete_core_ids = match kc_chain_metadata_repository
        .get_kc_ids_with_complete_metadata(blockchain_id.as_str(), contract_addr_str, &kc_ids)
        .await
    {
        Ok(ids) => ids,
        Err(error) => {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %error,
                "Failed to query complete core metadata rows"
            );
            HashSet::new()
        }
    };

    let mut ready = Vec::with_capacity(kcs_needing_sync.len());
    let mut waiting_for_metadata = Vec::new();
    let mut waiting_for_state = Vec::new();

    for (kc_id, ual) in kcs_needing_sync {
        if let Some(entry) = ready_by_id.get(kc_id) {
            ready.push((*kc_id, ual.clone(), entry.clone()));
            continue;
        }
        if complete_core_ids.contains(kc_id) {
            waiting_for_state.push(*kc_id);
        } else {
            waiting_for_metadata.push(*kc_id);
        }
    }

    tracing::trace!(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
        ready = ready_ids.len(),
        waiting_for_metadata = waiting_for_metadata.len(),
        waiting_for_state = waiting_for_state.len(),
        "SQL readiness gate results"
    );

    (ready, waiting_for_metadata, waiting_for_state)
}

#[instrument(
    name = "filter_check_local",
    skip(chunk, blockchain_id, contract_address, triple_store_assertions),
    fields(chunk_size = chunk.len())
)]
async fn check_local_existence(
    chunk: &[u64],
    blockchain_id: &BlockchainId,
    contract_address: &Address,
    triple_store_assertions: &TripleStoreAssertions,
) -> Vec<(u64, String)> {
    let kc_uals: Vec<(u64, String)> = chunk
        .iter()
        .map(|&kc_id| {
            let ual = derive_ual(blockchain_id, contract_address, kc_id as u128, None);
            (kc_id, ual)
        })
        .collect();

    let uals: Vec<String> = kc_uals.iter().map(|(_, ual)| ual.clone()).collect();
    let existing = triple_store_assertions
        .knowledge_collections_exist_by_uals(&uals)
        .await;

    kc_uals
        .into_iter()
        .filter_map(|(kc_id, ual)| {
            if existing.contains(&ual) {
                None
            } else {
                Some((kc_id, ual))
            }
        })
        .collect()
}
