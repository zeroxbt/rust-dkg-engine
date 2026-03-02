use std::collections::HashMap;

use dkg_blockchain::BlockchainId;
use dkg_repository::{KcSyncRepository, error::RepositoryError};

pub(crate) struct CleanupStaleQueueOutcome {
    pub(crate) stale_rows: usize,
    pub(crate) removed_rows: usize,
    pub(crate) failed_rows: usize,
    pub(crate) failed_contracts: usize,
}

pub(crate) async fn run(
    blockchain_id: &BlockchainId,
    batch_size: usize,
    kc_sync_repository: &KcSyncRepository,
) -> Result<CleanupStaleQueueOutcome, RepositoryError> {
    let stale_rows = kc_sync_repository
        .list_queue_keys_with_present_projection(blockchain_id.as_str(), batch_size.max(1))
        .await?;

    if stale_rows.is_empty() {
        return Ok(CleanupStaleQueueOutcome {
            stale_rows: 0,
            removed_rows: 0,
            failed_rows: 0,
            failed_contracts: 0,
        });
    }

    let mut stale_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    for (contract_address, kc_id) in stale_rows {
        stale_by_contract
            .entry(contract_address)
            .or_default()
            .push(kc_id);
    }

    let stale_rows_count: usize = stale_by_contract.values().map(Vec::len).sum();
    let mut removed_rows = 0usize;
    let mut failed_rows = 0usize;
    let mut failed_contracts = 0usize;

    for (contract_address, mut kc_ids) in stale_by_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        let count = kc_ids.len();
        match kc_sync_repository
            .remove_kcs(blockchain_id.as_str(), &contract_address, &kc_ids)
            .await
        {
            Ok(()) => {
                removed_rows = removed_rows.saturating_add(count);
            }
            Err(error) => {
                failed_rows = failed_rows.saturating_add(count);
                failed_contracts = failed_contracts.saturating_add(1);
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    contract_address = %contract_address,
                    count,
                    error = %error,
                    "KC reconciliation failed to remove stale queue rows"
                );
            }
        }
    }

    Ok(CleanupStaleQueueOutcome {
        stale_rows: stale_rows_count,
        removed_rows,
        failed_rows,
        failed_contracts,
    })
}
