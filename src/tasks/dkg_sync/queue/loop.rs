use std::{collections::HashSet, sync::Arc, time::Duration};

use dkg_blockchain::BlockchainId;
use dkg_observability as observability;
use tokio::sync::{Notify, mpsc, mpsc::error::TryRecvError};
use tokio_util::sync::CancellationToken;

use super::{dispatcher, outcomes};
use crate::tasks::dkg_sync::{
    DkgSyncConfig, DkgSyncDeps,
    pipeline::types::{QueueKcKey, QueueKcWorkItem, QueueOutcome},
};

const SYNC_RETRY_DELAY_SECS: i64 = 60;
const OUTCOME_CHANNEL_CLOSED_REASON: &str = "queue_outcome_channel_closed";

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    deps: DkgSyncDeps,
    config: DkgSyncConfig,
    notify: Arc<Notify>,
    blockchain_id: BlockchainId,
    input_tx: mpsc::Sender<Vec<QueueKcWorkItem>>,
    mut outcome_rx: mpsc::Receiver<Vec<QueueOutcome>>,
    shutdown: CancellationToken,
) {
    let fallback_retry_poll =
        Duration::from_secs(config.queue_processor.dispatch_idle_poll_secs.max(1));
    let mut draining = false;
    let mut outcome_channel_closed = false;
    let mut inflight: HashSet<QueueKcKey> = HashSet::new();

    loop {
        loop {
            match outcome_rx.try_recv() {
                Ok(outcomes) => {
                    outcomes::apply_queue_outcomes(
                        &deps,
                        &mut inflight,
                        notify.as_ref(),
                        &blockchain_id,
                        outcomes,
                        SYNC_RETRY_DELAY_SECS,
                        config.queue_processor.max_retry_attempts,
                    )
                    .await;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    outcome_channel_closed = true;
                    break;
                }
            }
        }

        if shutdown.is_cancelled() && !draining {
            draining = true;
            tracing::info!(
                blockchain_id = %blockchain_id,
                "DKG sync queue processor entering draining mode"
            );
        }
        if outcome_channel_closed && !draining {
            draining = true;
            tracing::warn!(
                blockchain_id = %blockchain_id,
                "DKG sync queue processor entering draining mode because outcome channel closed"
            );
        }

        let inflight_count = inflight.len();
        observability::record_sync_pipeline_inflight(inflight_count);

        if draining && inflight_count == 0 {
            tracing::info!(
                blockchain_id = %blockchain_id,
                "DKG sync queue processor drained in-flight work"
            );
            break;
        }
        if draining && outcome_channel_closed && inflight_count > 0 {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                inflight = inflight_count,
                "Outcome channel closed while inflight work remains; requeueing inflight KCs"
            );
            let fallback_outcomes: Vec<QueueOutcome> = inflight
                .iter()
                .cloned()
                .map(|key| {
                    QueueOutcome::retry_with_pending_error(key, OUTCOME_CHANNEL_CLOSED_REASON)
                })
                .collect();
            outcomes::apply_queue_outcomes(
                &deps,
                &mut inflight,
                notify.as_ref(),
                &blockchain_id,
                fallback_outcomes,
                SYNC_RETRY_DELAY_SECS,
                config.queue_processor.max_retry_attempts,
            )
            .await;
            continue;
        }

        let mut dispatched = false;
        if !draining && enough_peers_for_fetch(&deps, &blockchain_id) {
            if let Some(coalesce_wait) = dispatch_coalesce_delay(&config, inflight.len()) {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        draining = true;
                        tracing::info!(
                            blockchain_id = %blockchain_id,
                            "DKG sync queue processor entering draining mode during dispatch coalescing wait"
                        );
                        continue;
                    }
                    maybe_outcomes = outcome_rx.recv() => {
                        if let Some(outcomes) = maybe_outcomes {
                            outcomes::apply_queue_outcomes(
                                &deps,
                                &mut inflight,
                                notify.as_ref(),
                                &blockchain_id,
                                outcomes,
                                SYNC_RETRY_DELAY_SECS,
                                config.queue_processor.max_retry_attempts,
                            )
                            .await;
                        } else {
                            outcome_channel_closed = true;
                        }
                        continue;
                    }
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(coalesce_wait) => {}
                }
            }
            dispatched = dispatcher::dispatch_due_fifo(
                &config,
                &deps,
                &mut inflight,
                &input_tx,
                &blockchain_id,
            )
            .await;
        }

        if dispatched {
            continue;
        }

        tokio::select! {
            _ = shutdown.cancelled(), if !draining => {}
            maybe_outcomes = outcome_rx.recv() => {
                if let Some(outcomes) = maybe_outcomes {
                    outcomes::apply_queue_outcomes(
                        &deps,
                        &mut inflight,
                        notify.as_ref(),
                        &blockchain_id,
                        outcomes,
                        SYNC_RETRY_DELAY_SECS,
                        config.queue_processor.max_retry_attempts,
                    )
                    .await;
                } else {
                    outcome_channel_closed = true;
                }
            }
            _ = notify.notified() => {}
            _ = tokio::time::sleep(fallback_retry_poll) => {}
        }
    }
}

fn enough_peers_for_fetch(deps: &DkgSyncDeps, blockchain_id: &BlockchainId) -> bool {
    let total_shard_peers = deps.peer_registry.shard_peer_count(blockchain_id);
    let identified_peers = deps
        .peer_registry
        .identified_shard_peer_count(blockchain_id);
    let min_required = (total_shard_peers / 3).max(3);
    identified_peers >= min_required
}

fn dispatch_coalesce_delay(config: &DkgSyncConfig, inflight_len: usize) -> Option<Duration> {
    let inflight_limit = config.queue_processor.inflight_kc_limit.max(1);
    let dispatch_max_kc_per_attempt = config.queue_processor.dispatch_max_kc_per_attempt.max(1);
    let dispatch_target_kc_per_push = config
        .queue_processor
        .dispatch_target_kc_per_push
        .filter(|value| *value > 1)?;
    let dispatch_target_kc_per_push = dispatch_target_kc_per_push.min(dispatch_max_kc_per_attempt);
    let dispatch_coalesce_wait_ms = config
        .queue_processor
        .dispatch_coalesce_wait_ms
        .filter(|value| *value > 0)?;

    let free_slots = inflight_limit.saturating_sub(inflight_len);
    if free_slots == 0 || free_slots >= dispatch_target_kc_per_push {
        return None;
    }

    Some(Duration::from_millis(dispatch_coalesce_wait_ms))
}
