//! Insert stage: validates expiration and stores KCs in the triple store.
//!
//! Receives fetched KCs from the fetch stage, checks if they've expired,
//! fetches end epochs via Multicall3, and inserts valid KCs into the triple store.

use std::{collections::HashMap, sync::Arc, time::Instant};

use futures::future::join_all;
use tokio::sync::mpsc;

use crate::{
    managers::blockchain::{Address, BlockchainId, BlockchainManager},
    services::TripleStoreService,
};

use super::{FetchedKc, InsertStats};

/// Insert task: receives fetched KCs, checks expiration, inserts into triple store.
pub(crate) async fn insert_task(
    mut rx: mpsc::Receiver<Vec<FetchedKc>>,
    blockchain_id: BlockchainId,
    contract_address: Address,
    contract_addr_str: String,
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_service: Arc<TripleStoreService>,
) -> InsertStats {
    let task_start = Instant::now();
    let mut synced = Vec::new();
    let mut failed = Vec::new();
    let mut expired = Vec::new();
    let mut total_received = 0usize;

    // Get current epoch once for expiration checks
    let current_epoch = blockchain_manager
        .get_current_epoch(&blockchain_id)
        .await
        .ok();

    while let Some(batch) = rx.recv().await {
        let batch_start = Instant::now();
        total_received += batch.len();
        tracing::info!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            batch_size = batch.len(),
            total_received,
            elapsed_ms = task_start.elapsed().as_millis() as u64,
            "[DKG SYNC] Insert: received batch"
        );

        // Step 1: Fetch all end epochs using multicall (single RPC request)
        let kc_ids: Vec<u128> = batch.iter().map(|kc| kc.kc_id as u128).collect();
        let epoch_results = blockchain_manager
            .get_kc_end_epoch_batch(&blockchain_id, contract_address, &kc_ids)
            .await;

        // Build epoch map from batch results
        let epoch_map: HashMap<u64, Option<u64>> = match epoch_results {
            Ok(results) => results
                .into_iter()
                .map(|(kc_id, epoch)| (kc_id as u64, epoch))
                .collect(),
            Err(e) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    error = %e,
                    kc_count = kc_ids.len(),
                    "[DKG SYNC] Multicall to get end epochs failed, will retry later"
                );
                // Mark all as failed for retry
                for kc in batch {
                    failed.push(kc.kc_id);
                }
                continue;
            }
        };

        // Step 2: Filter by expiration
        let valid_kcs = filter_expired_kcs(
            batch,
            &epoch_map,
            current_epoch,
            &blockchain_id,
            &contract_addr_str,
            &mut failed,
            &mut expired,
        );

        // Step 3: Insert all valid KCs in parallel
        let (batch_synced, batch_failed) =
            insert_kcs_to_store(&valid_kcs, &triple_store_service, &blockchain_id, &contract_addr_str).await;

        synced.extend(batch_synced);
        failed.extend(batch_failed);

        tracing::info!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            batch_ms = batch_start.elapsed().as_millis() as u64,
            batch_synced = synced.len(),
            total_synced = synced.len(),
            "[DKG SYNC] Insert: batch completed"
        );
    }

    tracing::info!(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
        total_ms = task_start.elapsed().as_millis() as u64,
        synced = synced.len(),
        failed = failed.len(),
        expired = expired.len(),
        "[DKG SYNC] Insert task completed"
    );

    InsertStats {
        synced,
        failed,
        expired,
    }
}

/// Filter out expired KCs based on end epoch.
/// Returns valid KCs, mutates failed and expired vectors.
fn filter_expired_kcs(
    batch: Vec<FetchedKc>,
    epoch_map: &HashMap<u64, Option<u64>>,
    current_epoch: Option<u64>,
    blockchain_id: &BlockchainId,
    contract_addr_str: &str,
    failed: &mut Vec<u64>,
    expired: &mut Vec<u64>,
) -> Vec<FetchedKc> {
    let mut valid_kcs = Vec::new();

    for kc in batch {
        if let Some(current) = current_epoch {
            match epoch_map.get(&kc.kc_id) {
                Some(Some(end_epoch)) if current > *end_epoch => {
                    tracing::debug!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc.kc_id,
                        current_epoch = current,
                        end_epoch,
                        "[DKG SYNC] KC is expired, skipping insert"
                    );
                    expired.push(kc.kc_id);
                    continue;
                }
                Some(None) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc.kc_id,
                        "[DKG SYNC] Failed to get KC end epoch, will retry later"
                    );
                    failed.push(kc.kc_id);
                    continue;
                }
                None => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc.kc_id,
                        "[DKG SYNC] Missing epoch result, will retry later"
                    );
                    failed.push(kc.kc_id);
                    continue;
                }
                _ => {}
            }
        }
        valid_kcs.push(kc);
    }

    valid_kcs
}

/// Insert KCs into the triple store in parallel.
/// Returns (synced KC IDs, failed KC IDs).
async fn insert_kcs_to_store(
    valid_kcs: &[FetchedKc],
    triple_store_service: &TripleStoreService,
    blockchain_id: &BlockchainId,
    contract_addr_str: &str,
) -> (Vec<u64>, Vec<u64>) {
    let mut synced = Vec::new();
    let mut failed = Vec::new();

    // Triple store's internal semaphore provides backpressure
    let insert_futures: Vec<_> = valid_kcs
        .iter()
        .map(|kc| {
            let ts = triple_store_service;
            let ual = kc.ual.clone();
            let assertion = kc.assertion.clone();
            let metadata = kc.metadata.clone();
            let kc_id = kc.kc_id;
            async move {
                let result = ts
                    .insert_knowledge_collection(&ual, &assertion, &metadata)
                    .await;
                (kc_id, ual, result)
            }
        })
        .collect();

    let insert_results = join_all(insert_futures).await;

    for (kc_id, ual, result) in insert_results {
        match result {
            Ok(triples_inserted) => {
                tracing::debug!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    kc_id = kc_id,
                    ual = %ual,
                    triples = triples_inserted,
                    "[DKG SYNC] Stored synced KC in triple store"
                );
                synced.push(kc_id);
            }
            Err(e) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    kc_id = kc_id,
                    error = %e,
                    "[DKG SYNC] Failed to store KC in triple store"
                );
                failed.push(kc_id);
            }
        }
    }

    (synced, failed)
}
