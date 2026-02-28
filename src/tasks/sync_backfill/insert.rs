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

use super::types::{FetchedKc, QueueOutcome};
use crate::application::TripleStoreAssertions;

/// Insert stage: receives fetched KCs and inserts into triple store.
///
/// Expiration filtering is already performed in the filter stage, so all KCs
/// received here are valid and ready for insertion.
#[instrument(
    name = "sync_insert",
    skip(rx, triple_store_assertions, outcome_tx),
    fields(blockchain_id = %blockchain_id)
)]
pub(crate) async fn run_insert_stage(
    mut rx: mpsc::Receiver<Vec<FetchedKc>>,
    blockchain_id: BlockchainId,
    insert_batch_concurrency: usize,
    triple_store_assertions: Arc<TripleStoreAssertions>,
    outcome_tx: mpsc::Sender<Vec<QueueOutcome>>,
) {
    let task_start = Instant::now();
    let mut total_synced = 0usize;
    let mut total_failed = 0usize;
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
        let (remove_outcomes, retry_outcomes) = insert_kcs_to_store(
            batch,
            Arc::clone(&triple_store_assertions),
            blockchain_id.clone(),
            insert_batch_concurrency,
        )
        .await;

        let batch_synced_count = remove_outcomes.len();
        let batch_failed_count = retry_outcomes.len();
        total_synced += batch_synced_count;
        total_failed += batch_failed_count;
        observability::record_sync_kc_outcome(
            blockchain_id.as_str(),
            "insert",
            "synced",
            batch_synced_count,
        );
        observability::record_sync_kc_outcome(
            blockchain_id.as_str(),
            "insert",
            "failed",
            batch_failed_count,
        );

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

        if !remove_outcomes.is_empty() && outcome_tx.send(remove_outcomes).await.is_err() {
            return;
        }
        if !retry_outcomes.is_empty() && outcome_tx.send(retry_outcomes).await.is_err() {
            return;
        }

        tracing::debug!(
            batch_ms = batch_start.elapsed().as_millis() as u64,
            batch_synced = batch_synced_count,
            batch_failed = batch_failed_count,
            total_synced,
            total_failed,
            "Insert: batch completed"
        );
    }

    tracing::debug!(
        total_ms = task_start.elapsed().as_millis() as u64,
        total_synced,
        total_failed,
        "Insert stage completed"
    );
}

/// Insert KCs into the triple store in parallel.
/// Returns (remove outcomes, retry outcomes).
async fn insert_kcs_to_store(
    kcs: Vec<FetchedKc>,
    triple_store_assertions: Arc<TripleStoreAssertions>,
    blockchain_id: BlockchainId,
    insert_batch_concurrency: usize,
) -> (Vec<QueueOutcome>, Vec<QueueOutcome>) {
    let mut remove_outcomes = Vec::new();
    let mut retry_outcomes = Vec::new();

    // Bound in-flight inserts to avoid large bursts and long permit waits.
    let mut insert_results = stream::iter(kcs.into_iter())
        .map(|kc| {
            let ts = Arc::clone(&triple_store_assertions);
            let contract_addr_str = kc.contract_addr_str;
            let ual = kc.ual;
            let assertion = kc.assertion;
            let metadata = kc.metadata;
            let kc_id = kc.kc_id;
            async move {
                let result = ts
                    .insert_knowledge_collection(&ual, &assertion, &metadata, None)
                    .await;
                (contract_addr_str, kc_id, ual, result)
            }
        })
        .buffer_unordered(insert_batch_concurrency.max(1))
        .fuse();

    while let Some((contract_addr_str, kc_id, ual, result)) = insert_results.next().await {
        match result {
            Ok(triples_inserted) => {
                tracing::trace!(
                    kc_id = kc_id,
                    ual = %ual,
                    triples = triples_inserted,
                    "Stored synced KC in triple store"
                );
                remove_outcomes.push(QueueOutcome::remove(contract_addr_str, kc_id));
            }
            Err(e) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    kc_id = kc_id,
                    error = %e,
                    "Failed to store KC in triple store"
                );
                retry_outcomes.push(QueueOutcome::retry(contract_addr_str, kc_id));
            }
        }
    }

    (remove_outcomes, retry_outcomes)
}
