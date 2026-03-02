//! Filter stage: checks local existence and SQL readiness before fetch.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use dkg_blockchain::BlockchainId;
use dkg_domain::{KnowledgeCollectionMetadata, TokenIds, derive_ual};
use dkg_repository::{KcChainMetadataRepository, KcChainReadyKcStateMetadataEntry};
use tokio::sync::mpsc;
use tracing::instrument;

use dkg_observability as observability;

use crate::application::TripleStoreAssertions;
use crate::application::state_metadata::{BurnedMode, decode_burned_ids};
use crate::tasks::dkg_sync::pipeline::types::{
    KcToSync, QueueKcKey, QueueKcWorkItem, QueueOutcome,
};

#[allow(clippy::too_many_arguments)]
#[instrument(
    name = "sync_filter",
    skip(
        rx,
        kc_chain_metadata_repository,
        triple_store_assertions,
        tx,
        outcome_tx
    ),
    fields(blockchain_id = %blockchain_id)
)]
pub(crate) async fn run_filter_stage(
    mut rx: mpsc::Receiver<Vec<QueueKcWorkItem>>,
    filter_batch_size: usize,
    blockchain_id: BlockchainId,
    kc_chain_metadata_repository: KcChainMetadataRepository,
    triple_store_assertions: Arc<TripleStoreAssertions>,
    tx: mpsc::Sender<Vec<KcToSync>>,
    outcome_tx: mpsc::Sender<Vec<QueueOutcome>>,
) {
    let task_start = Instant::now();
    let filter_batch_size = filter_batch_size.max(1);

    while let Some(work_items) = rx.recv().await {
        for chunk in work_items.chunks(filter_batch_size) {
            let batch_result = process_filter_batch(
                chunk,
                &blockchain_id,
                &kc_chain_metadata_repository,
                &triple_store_assertions,
            )
            .await;

            observability::record_sync_kc_outcome(
                blockchain_id.as_str(),
                "filter",
                "already_synced",
                batch_result.already_synced.len(),
            );
            observability::record_sync_kc_outcome(
                blockchain_id.as_str(),
                "filter",
                "retry_later",
                batch_result.retry_later.len(),
            );

            if !batch_result.to_sync.is_empty() && tx.send(batch_result.to_sync).await.is_err() {
                tracing::trace!("Filter: fetch stage receiver dropped, stopping");
                return;
            }

            let mut outcomes = Vec::with_capacity(
                batch_result.already_synced.len() + batch_result.retry_later.len(),
            );
            outcomes.extend(
                batch_result
                    .already_synced
                    .into_iter()
                    .map(QueueOutcome::remove_already_synced),
            );
            outcomes.extend(
                batch_result.retry_later.into_iter().map(|key| {
                    QueueOutcome::retry_with_pending_error(key, "filter_stage_rejection")
                }),
            );

            if !outcomes.is_empty() && outcome_tx.send(outcomes).await.is_err() {
                tracing::trace!("Filter: queue outcome receiver dropped, stopping");
                return;
            }
        }
    }

    tracing::debug!(
        total_ms = task_start.elapsed().as_millis() as u64,
        "Filter stage completed"
    );
}

struct FilterBatchResult {
    already_synced: Vec<QueueKcKey>,
    retry_later: Vec<QueueKcKey>,
    to_sync: Vec<KcToSync>,
}

#[derive(Clone)]
struct PendingKc {
    key: QueueKcKey,
    ual: String,
}

#[instrument(
    name = "filter_batch",
    skip(
        chunk,
        blockchain_id,
        kc_chain_metadata_repository,
        triple_store_assertions
    ),
    fields(chunk_size = chunk.len())
)]
async fn process_filter_batch(
    chunk: &[QueueKcWorkItem],
    blockchain_id: &BlockchainId,
    kc_chain_metadata_repository: &KcChainMetadataRepository,
    triple_store_assertions: &TripleStoreAssertions,
) -> FilterBatchResult {
    let mut already_synced = Vec::new();

    let kcs_needing_sync =
        check_local_existence(chunk, blockchain_id, triple_store_assertions).await;

    let needing_sync_keys: HashSet<QueueKcKey> = kcs_needing_sync
        .iter()
        .map(|pending| pending.key.clone())
        .collect();
    for item in chunk {
        let key = item.key.clone();
        if !needing_sync_keys.contains(&key) {
            already_synced.push(key);
        }
    }

    if kcs_needing_sync.is_empty() {
        return FilterBatchResult {
            already_synced,
            retry_later: Vec::new(),
            to_sync: Vec::new(),
        };
    }

    let (ready, queued_without_ready_metadata) = gate_on_sql_readiness(
        &kcs_needing_sync,
        blockchain_id,
        kc_chain_metadata_repository,
    )
    .await;

    let mut retry_later = queued_without_ready_metadata;
    if !retry_later.is_empty() {
        let contracts: HashSet<&str> = retry_later
            .iter()
            .map(|key| key.contract_addr_str.as_str())
            .collect();
        tracing::error!(
            blockchain_id = %blockchain_id,
            missing_count = retry_later.len(),
            contracts = contracts.len(),
            "Invariant violation: KC queued without ready metadata/state; scheduling retry"
        );
    }

    if ready.is_empty() {
        return FilterBatchResult {
            already_synced,
            retry_later,
            to_sync: Vec::new(),
        };
    }

    let mut to_sync = Vec::with_capacity(ready.len());
    for (pending, entry) in ready {
        let Some(mode) = BurnedMode::from_raw(entry.burned_mode) else {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %pending.key.contract_addr_str,
                kc_id = pending.key.kc_id,
                burned_mode = entry.burned_mode,
                "Invalid burned mode in SQL state"
            );
            retry_later.push(pending.key.clone());
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
                contract = %pending.key.contract_addr_str,
                kc_id = pending.key.kc_id,
                "Failed to decode burned payload from SQL state"
            );
            retry_later.push(pending.key.clone());
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
            key: pending.key,
            ual: pending.ual,
            token_ids,
            merkle_root: Some(entry.latest_merkle_root),
            metadata,
        });
    }

    FilterBatchResult {
        already_synced,
        retry_later,
        to_sync,
    }
}

async fn gate_on_sql_readiness(
    kcs_needing_sync: &[PendingKc],
    blockchain_id: &BlockchainId,
    kc_chain_metadata_repository: &KcChainMetadataRepository,
) -> (
    Vec<(PendingKc, KcChainReadyKcStateMetadataEntry)>,
    Vec<QueueKcKey>,
) {
    if kcs_needing_sync.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let keys: Vec<(String, u64)> = kcs_needing_sync
        .iter()
        .map(|pending| (pending.key.contract_addr_str.clone(), pending.key.kc_id))
        .collect();
    let ready_by_id = match kc_chain_metadata_repository
        .get_many_ready_with_kc_state_metadata_for_keys(blockchain_id.as_str(), &keys)
        .await
    {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                error = %error,
                "Failed to query SQL ready rows"
            );
            HashMap::new()
        }
    };

    let mut ready = Vec::with_capacity(kcs_needing_sync.len());
    let mut queued_without_ready_metadata = Vec::new();

    for pending in kcs_needing_sync {
        let key = (pending.key.contract_addr_str.clone(), pending.key.kc_id);
        if let Some(entry) = ready_by_id.get(&key) {
            ready.push((pending.clone(), entry.clone()));
            continue;
        }
        // Queue rows should only exist after atomic metadata+state+enqueue persistence.
        // If this branch is hit, data was manually inserted, is legacy/inconsistent,
        // or an invariant was violated elsewhere.
        queued_without_ready_metadata.push(pending.key.clone());
    }

    tracing::trace!(
        blockchain_id = %blockchain_id,
        ready = ready.len(),
        queued_without_ready_metadata = queued_without_ready_metadata.len(),
        "SQL readiness gate results"
    );

    (ready, queued_without_ready_metadata)
}

#[instrument(
    name = "check_local_existence",
    skip(chunk, blockchain_id, triple_store_assertions),
    fields(chunk_size = chunk.len())
)]
async fn check_local_existence(
    chunk: &[QueueKcWorkItem],
    blockchain_id: &BlockchainId,
    triple_store_assertions: &TripleStoreAssertions,
) -> Vec<PendingKc> {
    if chunk.is_empty() {
        return Vec::new();
    }

    let mut pending = Vec::with_capacity(chunk.len());
    let mut uals = Vec::with_capacity(chunk.len());
    for item in chunk {
        let ual = derive_ual(
            blockchain_id,
            &item.contract_address,
            u128::from(item.key.kc_id),
            None,
        );
        uals.push(ual.clone());
        pending.push(PendingKc {
            key: item.key.clone(),
            ual,
        });
    }

    let existing = triple_store_assertions
        .knowledge_collections_exist_by_uals(&uals)
        .await;

    pending
        .into_iter()
        .filter(|pending| !existing.contains(&pending.ual))
        .collect()
}
