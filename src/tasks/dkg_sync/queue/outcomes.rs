use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dkg_blockchain::BlockchainId;
use dkg_observability as observability;
use tokio::sync::{Mutex, Notify};

use crate::tasks::{
    dkg_sync::pipeline::types::{
        ProjectionWriteAction, QueueKcKey, QueueOutcome, QueueWriteAction,
    },
    periodic::DkgSyncDeps,
};

pub(crate) async fn apply_queue_outcomes(
    deps: &DkgSyncDeps,
    inflight: &Arc<Mutex<HashSet<QueueKcKey>>>,
    notify: &Notify,
    blockchain_id: &BlockchainId,
    outcomes: Vec<QueueOutcome>,
    retry_delay_secs: i64,
) {
    if outcomes.is_empty() {
        return;
    }

    let mut remove_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    let mut retry_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    let mut present_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    let mut failed_by_reason_and_contract: HashMap<(&'static str, String), Vec<u64>> =
        HashMap::new();
    for outcome in &outcomes {
        match outcome.queue_action {
            QueueWriteAction::Remove => remove_by_contract
                .entry(outcome.key.contract_addr_str.clone())
                .or_default()
                .push(outcome.key.kc_id),
            QueueWriteAction::Retry => retry_by_contract
                .entry(outcome.key.contract_addr_str.clone())
                .or_default()
                .push(outcome.key.kc_id),
        }

        match outcome.projection_action {
            ProjectionWriteAction::Noop => {}
            ProjectionWriteAction::MarkPresent => present_by_contract
                .entry(outcome.key.contract_addr_str.clone())
                .or_default()
                .push(outcome.key.kc_id),
            ProjectionWriteAction::MarkFailed { reason } => failed_by_reason_and_contract
                .entry((reason, outcome.key.contract_addr_str.clone()))
                .or_default()
                .push(outcome.key.kc_id),
        }
    }

    for (contract, mut kc_ids) in remove_by_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        if let Err(error) = deps
            .kc_sync_repository
            .remove_kcs(blockchain_id.as_str(), &contract, &kc_ids)
            .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract,
                error = %error,
                "Failed to remove KCs from queue"
            );
        }
    }

    for (contract, mut kc_ids) in retry_by_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        if let Err(error) = deps
            .kc_sync_repository
            .increment_retry_count(blockchain_id.as_str(), &contract, &kc_ids, retry_delay_secs)
            .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract,
                error = %error,
                "Failed to increment retry count"
            );
        }
    }

    for ((reason, contract), mut kc_ids) in failed_by_reason_and_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        if let Err(error) = deps
            .kc_projection_repository
            .mark_failed(blockchain_id.as_str(), &contract, &kc_ids, reason)
            .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract,
                error = %error,
                "Failed to mark projection rows as failed for retried KCs"
            );
        }
    }

    for (contract, mut kc_ids) in present_by_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        if let Err(error) = deps
            .kc_projection_repository
            .mark_present(blockchain_id.as_str(), &contract, &kc_ids)
            .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract,
                error = %error,
                "Failed to mark projection rows as present"
            );
        }
    }

    {
        let mut inflight = inflight.lock().await;
        for outcome in outcomes {
            inflight.remove(&outcome.key);
        }
        observability::record_sync_pipeline_inflight(inflight.len());
    }

    notify.notify_waiters();
}
