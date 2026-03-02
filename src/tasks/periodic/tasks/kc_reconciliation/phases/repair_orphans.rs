use std::collections::HashMap;

use dkg_blockchain::BlockchainId;
use dkg_repository::{KcProjectionRepository, KcSyncRepository, error::RepositoryError};

const REASON_QUEUE_ORPHAN: &str = "reconcile_queue_orphan_projection_missing";

pub(crate) struct RepairOrphansOutcome {
    pub(crate) queue_missing_projection: usize,
    pub(crate) hydrated_pending: usize,
    pub(crate) pending_missing_queue: usize,
    pub(crate) reset_unknown: usize,
    pub(crate) failed_projection_updates: usize,
}

pub(crate) async fn run(
    blockchain_id: &BlockchainId,
    batch_size: usize,
    kc_projection_repository: &KcProjectionRepository,
    kc_sync_repository: &KcSyncRepository,
) -> Result<RepairOrphansOutcome, RepositoryError> {
    let queue_missing_projection = kc_sync_repository
        .list_queue_keys_missing_projection(blockchain_id.as_str(), batch_size.max(1))
        .await?;
    let pending_missing_queue = kc_projection_repository
        .list_pending_keys_missing_queue(blockchain_id.as_str(), batch_size.max(1))
        .await?;

    let mut queue_missing_projection_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    for (contract_address, kc_id) in queue_missing_projection {
        queue_missing_projection_by_contract
            .entry(contract_address)
            .or_default()
            .push(kc_id);
    }

    let mut pending_missing_queue_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    for (contract_address, kc_id) in pending_missing_queue {
        pending_missing_queue_by_contract
            .entry(contract_address)
            .or_default()
            .push(kc_id);
    }

    let mut hydrated_pending = 0usize;
    let mut reset_unknown = 0usize;
    let mut failed_projection_updates = 0usize;
    let queue_missing_projection_count: usize = queue_missing_projection_by_contract
        .values()
        .map(Vec::len)
        .sum();
    let pending_missing_queue_count: usize = pending_missing_queue_by_contract
        .values()
        .map(Vec::len)
        .sum();

    for (contract_address, mut kc_ids) in queue_missing_projection_by_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        let count = kc_ids.len();

        if let Err(error) = kc_projection_repository
            .ensure_desired_present(blockchain_id.as_str(), &contract_address, &kc_ids)
            .await
        {
            failed_projection_updates = failed_projection_updates.saturating_add(count);
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract_address = %contract_address,
                count,
                error = %error,
                "KC reconciliation failed to create projection rows for queue orphans"
            );
            continue;
        }

        if let Err(error) = kc_projection_repository
            .mark_pending_with_error(
                blockchain_id.as_str(),
                &contract_address,
                &kc_ids,
                Some(REASON_QUEUE_ORPHAN),
            )
            .await
        {
            failed_projection_updates = failed_projection_updates.saturating_add(count);
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract_address = %contract_address,
                count,
                error = %error,
                "KC reconciliation failed to mark queue-orphan projection rows as pending"
            );
            continue;
        }

        hydrated_pending = hydrated_pending.saturating_add(count);
    }

    for (contract_address, mut kc_ids) in pending_missing_queue_by_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        let count = kc_ids.len();

        if let Err(error) = kc_projection_repository
            .mark_unknown_from_pending(blockchain_id.as_str(), &contract_address, &kc_ids)
            .await
        {
            failed_projection_updates = failed_projection_updates.saturating_add(count);
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract_address = %contract_address,
                count,
                error = %error,
                "KC reconciliation failed to reset pending rows without queue back to unknown"
            );
            continue;
        }

        reset_unknown = reset_unknown.saturating_add(count);
    }

    Ok(RepairOrphansOutcome {
        queue_missing_projection: queue_missing_projection_count,
        hydrated_pending,
        pending_missing_queue: pending_missing_queue_count,
        reset_unknown,
        failed_projection_updates,
    })
}
