//! Insert stage: stores KCs in the triple store.
//!
//! Receives fetched KCs from the fetch stage and inserts them into the triple store.
//! Expiration filtering is already done in the filter stage.

use std::{sync::Arc, time::Instant};

use futures::future::join_all;
use tokio::sync::mpsc;
use tracing::instrument;

use super::types::{FetchedKc, InsertStats};
use crate::{managers::blockchain::BlockchainId, services::TripleStoreService};

/// Insert task: receives fetched KCs and inserts into triple store.
///
/// Expiration filtering is already performed in the filter stage, so all KCs
/// received here are valid and ready for insertion.
#[instrument(
    name = "sync_insert",
    skip(rx, triple_store_service),
    fields(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
    )
)]
pub(crate) async fn insert_task(
    mut rx: mpsc::Receiver<Vec<FetchedKc>>,
    blockchain_id: BlockchainId,
    contract_addr_str: String,
    triple_store_service: Arc<TripleStoreService>,
) -> InsertStats {
    let task_start = Instant::now();
    let mut synced = Vec::new();
    let mut failed = Vec::new();
    let mut total_received = 0usize;

    while let Some(batch) = rx.recv().await {
        let batch_start = Instant::now();
        total_received += batch.len();
        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            batch_size = batch.len(),
            total_received,
            elapsed_ms = task_start.elapsed().as_millis() as u64,
            "Insert: received batch"
        );

        // Insert all KCs in parallel
        let (batch_synced, batch_failed) = insert_kcs_to_store(
            &batch,
            &triple_store_service,
            &blockchain_id,
            &contract_addr_str,
        )
        .await;

        let batch_synced_count = batch_synced.len();
        let batch_failed_count = batch_failed.len();
        synced.extend(batch_synced);
        failed.extend(batch_failed);

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            batch_ms = batch_start.elapsed().as_millis() as u64,
            batch_synced = batch_synced_count,
            batch_failed = batch_failed_count,
            total_synced = synced.len(),
            "Insert: batch completed"
        );
    }

    tracing::debug!(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
        total_ms = task_start.elapsed().as_millis() as u64,
        synced = synced.len(),
        failed = failed.len(),
        "Insert task completed"
    );

    InsertStats { synced, failed }
}

/// Insert KCs into the triple store in parallel.
/// Returns (synced KC IDs, failed KC IDs).
async fn insert_kcs_to_store(
    kcs: &[FetchedKc],
    triple_store_service: &TripleStoreService,
    blockchain_id: &BlockchainId,
    contract_addr_str: &str,
) -> (Vec<u64>, Vec<u64>) {
    let mut synced = Vec::new();
    let mut failed = Vec::new();

    // Triple store's internal semaphore provides backpressure
    let insert_futures: Vec<_> = kcs
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
                tracing::trace!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
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
