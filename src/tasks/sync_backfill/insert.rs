//! Insert stage: stores KCs in the triple store.
//!
//! Receives fetched KCs from the fetch stage and inserts them into the triple store.
//! Expiration filtering is already done in the filter stage.

use std::{sync::Arc, time::Instant};

use dkg_blockchain::BlockchainId;
use dkg_observability as observability;
use futures::{StreamExt, stream};
use tokio::sync::mpsc;
use tracing::instrument;

use super::types::{FetchedKc, InsertStats};
use crate::application::TripleStoreAssertions;

/// Insert stage: receives fetched KCs and inserts into triple store.
///
/// Expiration filtering is already performed in the filter stage, so all KCs
/// received here are valid and ready for insertion.
#[instrument(
    name = "sync_insert",
    skip(rx, triple_store_assertions),
    fields(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
    )
)]
pub(crate) async fn run_insert_stage(
    mut rx: mpsc::Receiver<Vec<FetchedKc>>,
    blockchain_id: BlockchainId,
    contract_addr_str: String,
    insert_batch_concurrency: usize,
    triple_store_assertions: Arc<TripleStoreAssertions>,
) -> InsertStats {
    let task_start = Instant::now();
    let mut synced = Vec::new();
    let mut failed = Vec::new();
    let mut total_received = 0usize;

    while let Some(batch) = rx.recv().await {
        let batch_start = Instant::now();
        let batch_len = batch.len();
        let batch_assets: u64 = batch.iter().map(|kc| kc.estimated_assets).sum();
        total_received += batch_len;
        tracing::debug!(
            batch_size = batch_len,
            assets_in_batch = batch_assets,
            total_received,
            elapsed_ms = task_start.elapsed().as_millis() as u64,
            "Insert: received batch"
        );

        // Insert all KCs in parallel
        let (batch_synced, batch_failed) = insert_kcs_to_store(
            batch,
            Arc::clone(&triple_store_assertions),
            blockchain_id.clone(),
            contract_addr_str.clone(),
            insert_batch_concurrency,
        )
        .await;

        let batch_synced_count = batch_synced.len();
        let batch_failed_count = batch_failed.len();
        let batch_status = if batch_failed_count == 0 {
            "success"
        } else if batch_synced_count == 0 {
            "failed"
        } else {
            "partial"
        };
        observability::record_sync_insert_batch(
            batch_status,
            batch_start.elapsed(),
            batch_len,
            batch_assets,
        );
        synced.extend(batch_synced);
        failed.extend(batch_failed);

        tracing::debug!(
            batch_ms = batch_start.elapsed().as_millis() as u64,
            batch_synced = batch_synced_count,
            batch_failed = batch_failed_count,
            total_synced = synced.len(),
            "Insert: batch completed"
        );
    }

    tracing::debug!(
        total_ms = task_start.elapsed().as_millis() as u64,
        synced = synced.len(),
        failed = failed.len(),
        "Insert stage completed"
    );

    InsertStats { synced, failed }
}

/// Insert KCs into the triple store in parallel.
/// Returns (synced KC IDs, failed KC IDs).
async fn insert_kcs_to_store(
    kcs: Vec<FetchedKc>,
    triple_store_assertions: Arc<TripleStoreAssertions>,
    blockchain_id: BlockchainId,
    contract_addr_str: String,
    insert_batch_concurrency: usize,
) -> (Vec<u64>, Vec<u64>) {
    let mut synced = Vec::new();
    let mut failed = Vec::new();

    // Bound in-flight inserts to avoid large bursts and long permit waits.
    // Process completions as they arrive to avoid holding an extra results vec.
    let mut insert_results = stream::iter(kcs.into_iter())
        .map(|kc| {
            let ts = Arc::clone(&triple_store_assertions);
            let ual = kc.ual;
            let assertion = kc.assertion;
            let metadata = kc.metadata;
            let kc_id = kc.kc_id;
            async move {
                let result = ts
                    .insert_knowledge_collection(&ual, &assertion, &metadata, None)
                    .await;
                (kc_id, ual, result)
            }
        })
        .buffer_unordered(insert_batch_concurrency.max(1))
        .fuse();

    while let Some((kc_id, ual, result)) = insert_results.next().await {
        match result {
            Ok(triples_inserted) => {
                tracing::trace!(
                    kc_id = kc_id,
                    ual = %ual,
                    triples = triples_inserted,
                    "Stored synced KC in triple store"
                );
                synced.push(kc_id);
            }
            Err(e) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    kc_id = kc_id,
                    error = %e,
                    "Failed to store KC in triple store"
                );
                failed.push(kc_id);
            }
        }
    }

    (synced, failed)
}
