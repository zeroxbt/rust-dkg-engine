//! Insert stage: stores KCs in the triple store.
//!
//! Receives fetched KCs from the fetch stage and inserts them into the triple store.

use std::{sync::Arc, time::Instant};

use dkg_blockchain::BlockchainId;
use dkg_observability as observability;
use futures::{StreamExt, stream};
use tokio::sync::mpsc;
use tracing::instrument;

use crate::{
    application::KcMaterializationService,
    tasks::dkg_sync::pipeline::types::{FetchedKc, QueueOutcome},
};

const INSERT_STAGE_FAILURE_REASON: &str = "insert_stage_failure";

/// Insert stage: receives fetched KCs and inserts into triple store.
///
/// All KCs received here already passed filter-stage readiness checks.
#[instrument(
    name = "sync_insert",
    skip(rx, kc_materialization_service, outcome_tx),
    fields(blockchain_id = %blockchain_id)
)]
pub(crate) async fn run_insert_stage(
    mut rx: mpsc::Receiver<Vec<FetchedKc>>,
    blockchain_id: BlockchainId,
    insert_kc_concurrency: usize,
    kc_materialization_service: Arc<KcMaterializationService>,
    outcome_tx: mpsc::Sender<Vec<QueueOutcome>>,
) {
    while let Some(batch) = rx.recv().await {
        let batch_start = Instant::now();
        let batch_len = batch.len();
        let batch_assets: u64 = batch.iter().map(|kc| kc.estimated_assets).sum();
        tracing::debug!(
            blockchain_id = %blockchain_id,
            batch_size = batch_len,
            assets_in_batch = batch_assets,
            "Insert: received batch"
        );

        // Insert all KCs in parallel
        let (remove_outcomes, retry_outcomes) = insert_kcs_to_store(
            batch,
            Arc::clone(&kc_materialization_service),
            blockchain_id.clone(),
            insert_kc_concurrency,
        )
        .await;

        let batch_synced_count = remove_outcomes.len();
        let batch_failed_count = retry_outcomes.len();
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

        let remove_count = remove_outcomes.len();
        if remove_count > 0 && outcome_tx.send(remove_outcomes).await.is_err() {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                remove_count,
                "Insert: queue outcome receiver dropped while sending remove outcomes"
            );
            return;
        }
        let retry_count = retry_outcomes.len();
        if retry_count > 0 && outcome_tx.send(retry_outcomes).await.is_err() {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                retry_count,
                "Insert: queue outcome receiver dropped while sending retry outcomes"
            );
            return;
        }

        tracing::debug!(
            blockchain_id = %blockchain_id,
            batch_ms = batch_start.elapsed().as_millis() as u64,
            batch_synced = batch_synced_count,
            batch_failed = batch_failed_count,
            "Insert: batch completed"
        );
    }
}

/// Insert KCs into the triple store in parallel.
/// Returns (remove outcomes, retry outcomes).
async fn insert_kcs_to_store(
    kcs: Vec<FetchedKc>,
    kc_materialization_service: Arc<KcMaterializationService>,
    blockchain_id: BlockchainId,
    insert_kc_concurrency: usize,
) -> (Vec<QueueOutcome>, Vec<QueueOutcome>) {
    let mut remove_outcomes = Vec::new();
    let mut retry_outcomes = Vec::new();

    // Bound in-flight inserts to avoid large bursts and long permit waits.
    let mut insert_results = stream::iter(kcs.into_iter())
        .map(|kc| {
            let materializer = Arc::clone(&kc_materialization_service);
            let key = kc.key;
            let ual = kc.ual;
            let assertion = kc.assertion;
            let metadata = kc.metadata;
            async move {
                let result = materializer
                    .insert_knowledge_collection(&ual, &assertion, metadata.as_ref(), None)
                    .await;
                (key, ual, result)
            }
        })
        .buffer_unordered(insert_kc_concurrency.max(1))
        .fuse();

    while let Some((key, ual, result)) = insert_results.next().await {
        match result {
            Ok(triples_inserted) => {
                tracing::trace!(
                    contract = %key.contract_addr_str,
                    kc_id = key.kc_id,
                    ual = %ual,
                    triples = triples_inserted,
                    "Stored synced KC in triple store"
                );
                remove_outcomes.push(QueueOutcome::remove_synced(key));
            }
            Err(e) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    contract = %key.contract_addr_str,
                    kc_id = key.kc_id,
                    error = %e,
                    "Failed to store KC in triple store"
                );
                retry_outcomes.push(QueueOutcome::retry_with_pending_error(
                    key,
                    INSERT_STAGE_FAILURE_REASON,
                ));
            }
        }
    }

    (remove_outcomes, retry_outcomes)
}
