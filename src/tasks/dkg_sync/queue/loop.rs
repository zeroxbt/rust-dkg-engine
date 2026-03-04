use std::{collections::HashSet, sync::Arc, time::Duration};

use dkg_blockchain::BlockchainId;
use dkg_observability as observability;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::{dispatcher, outcomes};
use crate::tasks::dkg_sync::{
    DkgSyncConfig, DkgSyncDeps,
    pipeline::types::{QueueKcKey, QueueKcWorkItem, QueueOutcome},
};

const SYNC_RETRY_DELAY_SECS: i64 = 60;

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
    let mut inflight: HashSet<QueueKcKey> = HashSet::new();

    loop {
        while let Ok(outcomes) = outcome_rx.try_recv() {
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

        if shutdown.is_cancelled() && !draining {
            draining = true;
            tracing::info!(
                blockchain_id = %blockchain_id,
                "DKG sync queue processor entering draining mode"
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

        let mut dispatched = false;
        if !draining && enough_peers_for_fetch(&deps, &blockchain_id) {
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
