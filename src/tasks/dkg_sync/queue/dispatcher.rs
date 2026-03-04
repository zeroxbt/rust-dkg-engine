use std::collections::HashSet;

use chrono::Utc;
use dkg_blockchain::{Address, BlockchainId};
use dkg_observability as observability;
use tokio::sync::mpsc;

use crate::tasks::{
    dkg_sync::{
        DkgSyncConfig, DkgSyncDeps,
        pipeline::types::{QueueKcKey, QueueKcWorkItem},
    },
};

pub(crate) async fn dispatch_due_fifo(
    config: &DkgSyncConfig,
    deps: &DkgSyncDeps,
    inflight: &mut HashSet<QueueKcKey>,
    input_tx: &mpsc::Sender<Vec<QueueKcWorkItem>>,
    blockchain_id: &BlockchainId,
) -> bool {
    let dispatch_max_kc_per_attempt = config.queue_processor.dispatch_max_kc_per_attempt.max(1);
    let max_retry_attempts = config.queue_processor.max_retry_attempts.max(1);

    let free_slots = config
        .queue_processor
        .inflight_kc_limit
        .max(1)
        .saturating_sub(inflight.len());
    if free_slots == 0 {
        return false;
    }

    let now_ts = Utc::now().timestamp();
    let due_rows = match deps
        .kc_sync_repository
        .get_due_queue_entries_fifo_for_blockchain(
            blockchain_id.as_str(),
            now_ts,
            max_retry_attempts,
            free_slots.min(dispatch_max_kc_per_attempt) as u64,
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                error = %error,
                "Failed to read due sync queue rows"
            );
            return false;
        }
    };

    if due_rows.is_empty() {
        return false;
    }

    let mut accepted = Vec::with_capacity(due_rows.len());
    for row in due_rows {
        let Ok(contract_address) = row.contract_address.parse::<Address>() else {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract = %row.contract_address,
                kc_id = row.kc_id,
                "Skipping queue row for invalid contract address"
            );
            continue;
        };

        let key = QueueKcKey::new(row.contract_address, row.kc_id);
        if inflight.insert(key.clone()) {
            accepted.push(QueueKcWorkItem {
                key,
                contract_address,
            });
        }
    }
    observability::record_sync_pipeline_inflight(inflight.len());

    if accepted.is_empty() {
        return false;
    }

    let rollback_keys: Vec<QueueKcKey> = accepted.iter().map(|item| item.key.clone()).collect();

    if input_tx.send(accepted).await.is_err() {
        tracing::warn!(
            blockchain_id = %blockchain_id,
            "Sync pipeline input channel closed"
        );
        for key in rollback_keys {
            inflight.remove(&key);
        }
        observability::record_sync_pipeline_inflight(inflight.len());
        return false;
    }

    true
}
