//! Filter stage: checks local existence and RPC token ranges.
//!
//! Processes pending KCs in batches, filtering out those already synced locally,
//! then fetches token ranges via Multicall3 for remaining KCs.

use std::{collections::HashMap, sync::Arc, time::Instant};

use futures::future::join_all;
use tokio::sync::mpsc;

use crate::{
    managers::{
        blockchain::{Address, BlockchainId, BlockchainManager},
        triple_store::MAX_TOKENS_PER_KC,
    },
    services::TripleStoreService,
    utils::ual::derive_ual,
};

use super::{FilterStats, KcToSync, FILTER_BATCH_SIZE};

/// Filter task: processes pending KCs in batches, checking local existence first,
/// then RPC for token ranges only for KCs that need syncing.
/// Sends KCs that need syncing to fetch stage.
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
    let total_kcs = pending_kc_ids.len();
    let mut processed = 0usize;

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

        // Step 2: Only fetch token ranges for KCs that need syncing (RPC calls via multicall)
        if kcs_needing_sync.is_empty() {
            processed += chunk.len();
            continue;
        }

        let batch_to_sync = fetch_token_ranges(
            &kcs_needing_sync,
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
        "[DKG SYNC] Filter task completed"
    );

    FilterStats { already_synced }
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

/// Fetch token ranges for KCs via Multicall3 batch RPC call.
async fn fetch_token_ranges(
    kcs_needing_sync: &[(u64, String)],
    blockchain_id: &BlockchainId,
    contract_address: &Address,
    contract_addr_str: &str,
    blockchain_manager: &BlockchainManager,
) -> Vec<KcToSync> {
    // Build a map from kc_id to ual for later lookup
    let kc_id_to_ual: HashMap<u64, String> = kcs_needing_sync
        .iter()
        .map(|(kc_id, ual)| (*kc_id, ual.clone()))
        .collect();

    // Get all KC IDs for the batch call
    let kc_ids: Vec<u128> = kcs_needing_sync
        .iter()
        .map(|(kc_id, _)| *kc_id as u128)
        .collect();

    // Use multicall to fetch all token ranges in a single RPC request
    let range_results = blockchain_manager
        .get_knowledge_assets_range_batch(blockchain_id, *contract_address, &kc_ids)
        .await;

    // Process RPC results
    let mut batch_to_sync = Vec::new();

    match range_results {
        Ok(results) => {
            for (kc_id_u128, result) in results {
                let kc_id = kc_id_u128 as u64;
                let ual = match kc_id_to_ual.get(&kc_id) {
                    Some(u) => u.clone(),
                    None => continue,
                };

                match result {
                    Some((global_start, global_end, global_burned)) => {
                        let offset = (kc_id - 1) * MAX_TOKENS_PER_KC;
                        let start_token_id = global_start.saturating_sub(offset);
                        let end_token_id = global_end.saturating_sub(offset);
                        let burned: Vec<u64> = global_burned
                            .into_iter()
                            .map(|b| b.saturating_sub(offset))
                            .collect();

                        batch_to_sync.push(KcToSync {
                            kc_id,
                            ual,
                            start_token_id,
                            end_token_id,
                            burned,
                        });
                    }
                    None => {
                        tracing::warn!(
                            blockchain_id = %blockchain_id,
                            contract = %contract_addr_str,
                            kc_id = kc_id,
                            "[DKG SYNC] KC has no token range on chain, skipping"
                        );
                    }
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                kc_count = kc_ids.len(),
                "[DKG SYNC] Multicall to get KC token ranges failed, will retry later"
            );
        }
    }

    batch_to_sync
}
