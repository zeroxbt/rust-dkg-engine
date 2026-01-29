//! Filter stage: checks local existence, fetches RPC data, and filters expired KCs.
//!
//! Processes pending KCs in batches:
//! 1. Filters out those already synced locally
//! 2. Fetches end epochs and filters out expired KCs
//! 3. Fetches token ranges and merkle roots for non-expired KCs
//! 4. Sends remaining KCs to fetch stage
//!
//! All RPC calls are consolidated in this stage to minimize latency.

use std::{sync::Arc, time::Instant};

use futures::future::join_all;
use tokio::sync::mpsc;
use tracing::instrument;

use crate::{
    managers::{
        blockchain::{
            Address, BlockchainId, BlockchainManager,
            multicall::{MulticallBatch, MulticallRequest, encoders},
        },
        triple_store::MAX_TOKENS_PER_KC,
    },
    services::TripleStoreService,
    utils::ual::derive_ual,
};

use super::{FilterStats, KcToSync, FILTER_BATCH_SIZE};

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
        // Step 1: Check local existence by UAL first (cheap, no RPC needed)
        let kcs_needing_sync =
            check_local_existence(chunk, &blockchain_id, &contract_address, &triple_store_service)
                .await;

        // Track already synced
        let needing_sync_ids: std::collections::HashSet<u64> =
            kcs_needing_sync.iter().map(|(id, _)| *id).collect();
        for &kc_id in chunk {
            if !needing_sync_ids.contains(&kc_id) {
                already_synced.push(kc_id);
            }
        }

        if kcs_needing_sync.is_empty() {
            processed += chunk.len();
            continue;
        }

        // Step 2: Fetch end epochs FIRST and filter expired (single Multicall3 RPC call)
        // This avoids fetching token ranges for expired KCs, saving bandwidth
        let non_expired_kcs = fetch_end_epochs_and_filter(
            &kcs_needing_sync,
            current_epoch,
            &blockchain_id,
            &contract_address,
            &contract_addr_str,
            &blockchain_manager,
            &mut expired,
        )
        .await;

        if non_expired_kcs.is_empty() {
            processed += chunk.len();
            continue;
        }

        // Step 3: Fetch token ranges and merkle roots for non-expired KCs (single Multicall3 RPC call)
        let batch_to_sync = fetch_token_ranges_and_merkle_roots(
            &non_expired_kcs,
            &blockchain_id,
            &contract_address,
            &contract_addr_str,
            &blockchain_manager,
        )
        .await;

        // Send to fetch stage (blocks if channel full - backpressure)
        processed += chunk.len();
        if !batch_to_sync.is_empty() {
            tracing::info!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                batch_size = batch_to_sync.len(),
                processed,
                total_kcs,
                elapsed_ms = task_start.elapsed().as_millis() as u64,
                "[DKG SYNC] Filter: sending batch to fetch stage"
            );
            if tx.send(batch_to_sync).await.is_err() {
                tracing::debug!("[DKG SYNC] Filter: fetch stage receiver dropped, stopping");
                break;
            }
        }
    }

    tracing::info!(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
        total_ms = task_start.elapsed().as_millis() as u64,
        already_synced = already_synced.len(),
        expired = expired.len(),
        "[DKG SYNC] Filter task completed"
    );

    FilterStats { already_synced, expired }
}

/// Check which KCs already exist locally in the triple store.
/// Returns only those that need syncing (don't exist locally).
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

    // Check existence in parallel - triple store's internal semaphore provides backpressure
    let existence_futures: Vec<_> = kc_uals
        .iter()
        .map(|(kc_id, ual)| {
            let ts = triple_store_service;
            let ual = ual.clone();
            let kc_id = *kc_id;
            async move {
                let exists = ts.knowledge_collection_exists_by_ual(&ual).await;
                (kc_id, ual, exists)
            }
        })
        .collect();

    let existence_results = join_all(existence_futures).await;

    // Return only KCs that need syncing
    existence_results
        .into_iter()
        .filter_map(|(kc_id, ual, exists)| if exists { None } else { Some((kc_id, ual)) })
        .collect()
}

/// Fetch end epochs for KCs to filter expired ones early.
///
/// Returns (kc_id, ual) for non-expired KCs. Expired KCs are added to the expired list.
async fn fetch_end_epochs_and_filter(
    kcs_needing_sync: &[(u64, String)],
    current_epoch: Option<u64>,
    blockchain_id: &BlockchainId,
    contract_address: &Address,
    contract_addr_str: &str,
    blockchain_manager: &BlockchainManager,
    expired: &mut Vec<u64>,
) -> Vec<(u64, String)> {
    let kc_ids: Vec<u128> = kcs_needing_sync
        .iter()
        .map(|(kc_id, _)| *kc_id as u128)
        .collect();

    // Fetch end epochs via Multicall3
    let mut batch = MulticallBatch::with_capacity(kc_ids.len());
    for &kc_id in &kc_ids {
        batch.add(MulticallRequest::new(
            *contract_address,
            encoders::encode_get_end_epoch(kc_id),
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
                "[DKG SYNC] Multicall to get end epochs failed, will retry later"
            );
            return Vec::new();
        }
    };

    // Filter expired KCs immediately
    let mut non_expired = Vec::new();
    for (i, (kc_id, ual)) in kcs_needing_sync.iter().enumerate() {
        let end_epoch = results[i].as_u64().filter(|&e| e != 0);

        // Check if expired
        if let (Some(current), Some(end)) = (current_epoch, end_epoch) {
            if current > end {
                tracing::debug!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    kc_id = kc_id,
                    current_epoch = current,
                    end_epoch = end,
                    "[DKG SYNC] Filter: KC is expired, skipping"
                );
                expired.push(*kc_id);
                continue;
            }
        }

        non_expired.push((*kc_id, ual.clone()));
    }

    non_expired
}

/// Fetch token ranges and merkle roots for non-expired KCs via Multicall3.
///
/// Batches both calls together: for each KC we fetch getKnowledgeAssetsRange and getLatestMerkleRoot.
async fn fetch_token_ranges_and_merkle_roots(
    kcs_needing_sync: &[(u64, String)],
    blockchain_id: &BlockchainId,
    contract_address: &Address,
    contract_addr_str: &str,
    blockchain_manager: &BlockchainManager,
) -> Vec<KcToSync> {
    if kcs_needing_sync.is_empty() {
        return Vec::new();
    }

    let kc_ids: Vec<u128> = kcs_needing_sync.iter().map(|(id, _)| *id as u128).collect();

    // Batch both token ranges AND merkle roots in a single Multicall3
    // Layout: [range_0, merkle_0, range_1, merkle_1, ...]
    let mut batch = MulticallBatch::with_capacity(kc_ids.len() * 2);
    for &kc_id in &kc_ids {
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
                "[DKG SYNC] Multicall to get token ranges and merkle roots failed, will retry later"
            );
            return Vec::new();
        }
    };

    // Process results (2 results per KC: range, merkle_root)
    let mut batch_to_sync = Vec::new();
    for (i, (kc_id, ual)) in kcs_needing_sync.iter().enumerate() {
        let range_idx = i * 2;
        let merkle_idx = i * 2 + 1;

        let range = results[range_idx].as_knowledge_assets_range();
        let Some((global_start, global_end, global_burned)) = range else {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                kc_id = kc_id,
                "[DKG SYNC] KC has no token range on chain, skipping"
            );
            continue;
        };

        let merkle_root = results[merkle_idx].as_bytes32_hex();

        let offset = (*kc_id - 1) * MAX_TOKENS_PER_KC;
        let start_token_id = global_start.saturating_sub(offset);
        let end_token_id = global_end.saturating_sub(offset);
        let burned: Vec<u64> = global_burned
            .into_iter()
            .map(|b| b.saturating_sub(offset))
            .collect();

        batch_to_sync.push(KcToSync {
            kc_id: *kc_id,
            ual: ual.clone(),
            start_token_id,
            end_token_id,
            burned,
            merkle_root,
        });
    }

    batch_to_sync
}
