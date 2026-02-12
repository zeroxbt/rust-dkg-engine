//! Filter stage: checks local existence, fetches RPC data, and filters expired KCs.
//!
//! Processes pending KCs in batches:
//! 1. Filters out those already synced locally
//! 2. Fetches all RPC data in a single Multicall (end epochs, token ranges, merkle roots)
//! 3. Filters out expired KCs from results
//! 4. Sends remaining KCs to fetch stage
//!
//! All RPC calls are consolidated into a single Multicall per batch to minimize latency.

use std::{sync::Arc, time::Instant};

use tokio::sync::mpsc;
use tracing::instrument;

use super::{
    FILTER_BATCH_SIZE,
    types::{FilterStats, KcToSync},
};
use crate::{
    managers::blockchain::{
        Address, BlockchainId, BlockchainManager,
        multicall::{MulticallBatch, MulticallRequest, encoders},
    },
    services::TripleStoreService,
    types::{TokenIds, derive_ual},
};

/// Filter task: processes pending KCs in batches, checking local existence first,
/// fetching token ranges and end epochs, then filtering expired KCs.
/// Sends non-expired KCs that need syncing to fetch stage.
#[instrument(
    name = "sync_filter",
    skip(pending_kc_ids, blockchain_manager, triple_store_service, tx),
    fields(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
        pending_count = pending_kc_ids.len(),
    )
)]
pub(crate) async fn filter_task(
    pending_kc_ids: Vec<u64>,
    blockchain_id: BlockchainId,
    contract_address: Address,
    contract_addr_str: String,
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_service: Arc<TripleStoreService>,
    tx: mpsc::Sender<Vec<KcToSync>>,
) -> FilterStats {
    let task_start = Instant::now();
    let mut already_synced = Vec::new();
    let mut expired = Vec::new();
    let total_kcs = pending_kc_ids.len();
    let mut processed = 0usize;

    // Get current epoch once for expiration filtering (single RPC call)
    let current_epoch = blockchain_manager
        .get_current_epoch(&blockchain_id)
        .await
        .ok();

    for chunk in pending_kc_ids.chunks(FILTER_BATCH_SIZE) {
        let batch_result = process_filter_batch(
            chunk,
            &blockchain_id,
            &contract_address,
            &triple_store_service,
            current_epoch,
            &contract_addr_str,
            &blockchain_manager,
        )
        .await;

        // Track already synced
        already_synced.extend(batch_result.already_synced);
        expired.extend(batch_result.expired);
        processed += chunk.len();

        // Send to fetch stage (blocks if channel full - backpressure)
        if !batch_result.to_sync.is_empty() {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                batch_size = batch_result.to_sync.len(),
                processed,
                total_kcs,
                elapsed_ms = task_start.elapsed().as_millis() as u64,
                "Filter: sending batch to fetch stage"
            );
            if tx.send(batch_result.to_sync).await.is_err() {
                tracing::debug!("Filter: fetch stage receiver dropped, stopping");
                break;
            }
        }
    }

    tracing::debug!(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
        total_ms = task_start.elapsed().as_millis() as u64,
        already_synced = already_synced.len(),
        expired = expired.len(),
        "Filter task completed"
    );

    FilterStats {
        already_synced,
        expired,
    }
}

/// Result of processing a single filter batch.
struct FilterBatchResult {
    already_synced: Vec<u64>,
    expired: Vec<u64>,
    to_sync: Vec<KcToSync>,
}

/// Process a single batch in the filter stage.
#[instrument(
    name = "filter_batch",
    skip(chunk, blockchain_id, contract_address, triple_store_service, blockchain_manager),
    fields(chunk_size = chunk.len())
)]
async fn process_filter_batch(
    chunk: &[u64],
    blockchain_id: &BlockchainId,
    contract_address: &Address,
    triple_store_service: &TripleStoreService,
    current_epoch: Option<u64>,
    contract_addr_str: &str,
    blockchain_manager: &BlockchainManager,
) -> FilterBatchResult {
    let mut already_synced = Vec::new();
    let mut expired = Vec::new();

    // Step 1: Check local existence by UAL first (cheap, no RPC needed)
    let kcs_needing_sync =
        check_local_existence(chunk, blockchain_id, contract_address, triple_store_service).await;

    // Track already synced
    let needing_sync_ids: std::collections::HashSet<u64> =
        kcs_needing_sync.iter().map(|(id, _)| *id).collect();
    for &kc_id in chunk {
        if !needing_sync_ids.contains(&kc_id) {
            already_synced.push(kc_id);
        }
    }

    if kcs_needing_sync.is_empty() {
        return FilterBatchResult {
            already_synced,
            expired,
            to_sync: Vec::new(),
        };
    }

    // Step 2: Fetch all RPC data in a single Multicall and filter expired
    let to_sync = fetch_rpc_data_and_filter(
        &kcs_needing_sync,
        current_epoch,
        blockchain_id,
        contract_address,
        contract_addr_str,
        blockchain_manager,
        &mut expired,
    )
    .await;

    FilterBatchResult {
        already_synced,
        expired,
        to_sync,
    }
}

/// Check which KCs already exist locally in the triple store.
/// Returns only those that need syncing (don't exist locally).
#[instrument(
    name = "filter_check_local",
    skip(chunk, blockchain_id, contract_address, triple_store_service),
    fields(chunk_size = chunk.len())
)]
async fn check_local_existence(
    chunk: &[u64],
    blockchain_id: &BlockchainId,
    contract_address: &Address,
    triple_store_service: &TripleStoreService,
) -> Vec<(u64, String)> {
    // Build UALs for all KCs in the chunk
    let kc_uals: Vec<(u64, String)> = chunk
        .iter()
        .map(|&kc_id| {
            let ual = derive_ual(blockchain_id, contract_address, kc_id as u128, None);
            (kc_id, ual)
        })
        .collect();

    // Batch existence check to reduce per-KC SPARQL requests
    let uals: Vec<String> = kc_uals.iter().map(|(_, ual)| ual.clone()).collect();
    let existing = triple_store_service
        .knowledge_collections_exist_by_uals(&uals)
        .await;

    // Return only KCs that need syncing
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

/// Fetch all RPC data for KCs in a single Multicall and filter expired ones.
///
/// Batches end epochs, token ranges, and merkle roots together:
/// Layout: [end_epoch_0, range_0, merkle_0, end_epoch_1, range_1, merkle_1, ...]
///
/// Returns KcToSync for non-expired KCs. Expired KCs are added to the expired list.
#[instrument(
    name = "filter_fetch_rpc",
    skip(kcs_needing_sync, blockchain_manager, expired),
    fields(kc_count = kcs_needing_sync.len())
)]
async fn fetch_rpc_data_and_filter(
    kcs_needing_sync: &[(u64, String)],
    current_epoch: Option<u64>,
    blockchain_id: &BlockchainId,
    contract_address: &Address,
    contract_addr_str: &str,
    blockchain_manager: &BlockchainManager,
    expired: &mut Vec<u64>,
) -> Vec<KcToSync> {
    if kcs_needing_sync.is_empty() {
        return Vec::new();
    }

    let kc_ids: Vec<u128> = kcs_needing_sync
        .iter()
        .map(|(kc_id, _)| *kc_id as u128)
        .collect();

    // Batch all calls in a single Multicall3
    // Layout: [end_epoch_0, range_0, merkle_0, end_epoch_1, range_1, merkle_1, ...]
    let mut batch = MulticallBatch::with_capacity(kc_ids.len() * 3);
    for &kc_id in &kc_ids {
        batch.add(MulticallRequest::new(
            *contract_address,
            encoders::encode_get_end_epoch(kc_id),
        ));
        batch.add(MulticallRequest::new(
            *contract_address,
            encoders::encode_get_knowledge_assets_range(kc_id),
        ));
        batch.add(MulticallRequest::new(
            *contract_address,
            encoders::encode_get_merkle_root(kc_id),
        ));
    }

    let results = match blockchain_manager
        .execute_multicall(blockchain_id, batch)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                kc_count = kc_ids.len(),
                "Multicall failed, will retry later"
            );
            return Vec::new();
        }
    };

    // Process results (3 results per KC: end_epoch, range, merkle_root)
    let mut batch_to_sync = Vec::new();
    for ((kc_id, ual), chunk) in kcs_needing_sync.iter().zip(results.chunks(3)) {
        let [epoch_result, range_result, merkle_result] = chunk else {
            continue;
        };

        // Check expiration first
        let end_epoch = epoch_result.as_u64().filter(|&e| e != 0);
        if let (Some(current), Some(end)) = (current_epoch, end_epoch)
            && current > end
        {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                kc_id = kc_id,
                current_epoch = current,
                end_epoch = end,
                "Filter: KC is expired, skipping"
            );
            expired.push(*kc_id);
            continue;
        }

        // Parse token range
        let Some((global_start, global_end, global_burned)) =
            range_result.as_knowledge_assets_range()
        else {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                kc_id = kc_id,
                "KC has no token range on chain, skipping"
            );
            continue;
        };

        let merkle_root = merkle_result.as_bytes32_hex();
        let token_ids =
            TokenIds::from_global_range(*kc_id as u128, global_start, global_end, global_burned);

        batch_to_sync.push(KcToSync {
            kc_id: *kc_id,
            ual: ual.clone(),
            token_ids,
            merkle_root,
        });
    }

    batch_to_sync
}
