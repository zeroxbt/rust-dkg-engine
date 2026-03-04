use std::collections::{HashMap, HashSet};

use dkg_blockchain::BlockchainId;
use dkg_observability as observability;
use tokio::sync::Notify;

use crate::tasks::{
    dkg_sync::{
        DkgSyncDeps,
        pipeline::types::{
        ProjectionWriteAction, QueueKcKey, QueueOutcome, QueueWriteAction,
        },
    },
};

const MAX_RETRY_EXCEEDED_REASON: &str = "max_retry_exceeded";

pub(crate) async fn apply_queue_outcomes(
    deps: &DkgSyncDeps,
    inflight: &mut HashSet<QueueKcKey>,
    notify: &Notify,
    blockchain_id: &BlockchainId,
    outcomes: Vec<QueueOutcome>,
    retry_delay_secs: i64,
    max_retry_attempts: u32,
) {
    if outcomes.is_empty() {
        return;
    }

    let mut remove_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    let mut retry_by_reason_and_contract: HashMap<(Option<&'static str>, String), Vec<u64>> =
        HashMap::new();
    let mut present_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    for outcome in &outcomes {
        match outcome.queue_action {
            QueueWriteAction::Remove => remove_by_contract
                .entry(outcome.key.contract_addr_str.clone())
                .or_default()
                .push(outcome.key.kc_id),
            QueueWriteAction::Retry => retry_by_reason_and_contract
                .entry((
                    match outcome.projection_action {
                        ProjectionWriteAction::MarkPending { last_error } => last_error,
                        ProjectionWriteAction::MarkPresent => None,
                    },
                    outcome.key.contract_addr_str.clone(),
                ))
                .or_default()
                .push(outcome.key.kc_id),
        }

        match outcome.projection_action {
            ProjectionWriteAction::MarkPresent => present_by_contract
                .entry(outcome.key.contract_addr_str.clone())
                .or_default()
                .push(outcome.key.kc_id),
            ProjectionWriteAction::MarkPending { .. } => {}
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

    // `0` means "do not retry" but the first processing attempt is still allowed.
    let max_retry_attempts = max_retry_attempts.max(1);
    for ((reason, contract), mut kc_ids) in retry_by_reason_and_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();

        let retry_counts = match deps
            .kc_sync_repository
            .get_retry_counts_for_kcs(blockchain_id.as_str(), &contract, &kc_ids)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract,
                    error = %error,
                    "Failed to read retry counts for queue outcomes"
                );
                HashMap::new()
            }
        };

        let mut retry_ids = Vec::new();
        let mut terminal_ids = Vec::new();
        for kc_id in kc_ids {
            let will_exceed = retry_counts
                .get(&kc_id)
                .is_some_and(|count| count.saturating_add(1) >= max_retry_attempts);
            if will_exceed {
                terminal_ids.push(kc_id);
            } else {
                retry_ids.push(kc_id);
            }
        }

        if !retry_ids.is_empty() {
            if let Err(error) = deps
                .kc_sync_repository
                .increment_retry_count(
                    blockchain_id.as_str(),
                    &contract,
                    &retry_ids,
                    retry_delay_secs,
                )
                .await
            {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract,
                    error = %error,
                    "Failed to increment retry count"
                );
            } else if let Err(error) = deps
                .kc_projection_repository
                .mark_pending_with_error(blockchain_id.as_str(), &contract, &retry_ids, reason)
                .await
            {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract,
                    error = %error,
                    "Failed to mark projection rows as pending after retry scheduling"
                );
            }
        }

        if !terminal_ids.is_empty() {
            let terminal_reason = reason.unwrap_or(MAX_RETRY_EXCEEDED_REASON);
            if let Err(error) = deps
                .kc_projection_repository
                .mark_failed(
                    blockchain_id.as_str(),
                    &contract,
                    &terminal_ids,
                    terminal_reason,
                )
                .await
            {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract,
                    error = %error,
                    "Failed to mark terminally failed projection rows"
                );
            } else if let Err(error) = deps
                .kc_sync_repository
                .remove_kcs(blockchain_id.as_str(), &contract, &terminal_ids)
                .await
            {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract,
                    error = %error,
                    "Failed to remove terminally failed KCs from queue"
                );
            }
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

    for outcome in outcomes {
        inflight.remove(&outcome.key);
    }
    observability::record_sync_pipeline_inflight(inflight.len());

    notify.notify_waiters();
}
