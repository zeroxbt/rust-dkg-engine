use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use dkg_blockchain::{Address, BlockchainManager};
use dkg_domain::BlockchainId;
use dkg_observability as observability;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::{tasks::periodic::ClaimRewardsDeps, tasks::periodic::runner::run_with_shutdown};

/// Interval between claim rewards cycles (1 hour).
pub(crate) const CLAIM_REWARDS_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Maximum number of delegators claimed per transaction.
pub(crate) const CLAIM_REWARDS_BATCH_SIZE: usize = 10;

pub(crate) struct ClaimRewardsTask {
    blockchain_manager: Arc<BlockchainManager>,
}

impl ClaimRewardsTask {
    pub(crate) fn new(deps: ClaimRewardsDeps) -> Self {
        Self {
            blockchain_manager: deps.blockchain_manager,
        }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("claim_rewards", shutdown, || self.execute(blockchain_id)).await;
    }

    #[tracing::instrument(name = "periodic_tasks.claim_rewards", skip(self))]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let cycle_started = Instant::now();
        let identity_id = self.blockchain_manager.identity_id(blockchain_id);

        let delegators = match self
            .blockchain_manager
            .get_delegators(blockchain_id, identity_id)
            .await
        {
            Ok(delegators) => delegators,
            Err(e) => {
                observability::record_claim_rewards_cycle(
                    blockchain_id.as_str(),
                    "delegators_error",
                    cycle_started.elapsed(),
                    0,
                    0,
                    0,
                    0,
                    0,
                );
                tracing::warn!(error = %e, "Failed to fetch delegators");
                return CLAIM_REWARDS_INTERVAL;
            }
        };

        let delegator_count = delegators.len();
        if delegators.is_empty() {
            observability::record_claim_rewards_cycle(
                blockchain_id.as_str(),
                "no_delegators",
                cycle_started.elapsed(),
                0,
                0,
                0,
                0,
                0,
            );
            tracing::debug!("No delegators found");
            return CLAIM_REWARDS_INTERVAL;
        }

        let current_epoch = match self
            .blockchain_manager
            .get_current_epoch(blockchain_id)
            .await
        {
            Ok(epoch) => epoch,
            Err(e) => {
                observability::record_claim_rewards_cycle(
                    blockchain_id.as_str(),
                    "epoch_error",
                    cycle_started.elapsed(),
                    delegator_count,
                    0,
                    0,
                    0,
                    0,
                );
                tracing::warn!(error = %e, "Failed to get current epoch");
                return CLAIM_REWARDS_INTERVAL;
            }
        };

        let mut last_claimed_epochs: HashMap<u64, Vec<Address>> = HashMap::new();
        let mut last_claimed_futures = FuturesUnordered::new();

        for delegator in delegators {
            let blockchain_id = blockchain_id.clone();
            let blockchain_manager = Arc::clone(&self.blockchain_manager);
            last_claimed_futures.push(async move {
                let epoch = blockchain_manager
                    .get_last_claimed_epoch(&blockchain_id, identity_id, delegator)
                    .await;
                (delegator, epoch)
            });
        }

        while let Some((delegator, result)) = last_claimed_futures.next().await {
            match result {
                Ok(epoch) => {
                    last_claimed_epochs
                        .entry(epoch)
                        .or_default()
                        .push(delegator);
                }
                Err(e) => {
                    tracing::warn!(error = %e, delegator = %delegator, "Failed to get last claimed epoch");
                }
            }
        }

        if let Some(zero_delegators) = last_claimed_epochs.get(&0).cloned() {
            let target_epoch = current_epoch.saturating_sub(1);
            let mut has_ever_futures = FuturesUnordered::new();

            for delegator in zero_delegators {
                let blockchain_id = blockchain_id.clone();
                let blockchain_manager = Arc::clone(&self.blockchain_manager);
                has_ever_futures.push(async move {
                    let has_ever = blockchain_manager
                        .has_ever_delegated_to_node(&blockchain_id, identity_id, delegator)
                        .await;
                    (delegator, has_ever)
                });
            }

            while let Some((delegator, result)) = has_ever_futures.next().await {
                match result {
                    Ok(false) => {
                        last_claimed_epochs
                            .entry(target_epoch)
                            .or_default()
                            .push(delegator);
                    }
                    Ok(true) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, delegator = %delegator, "Failed to check delegation history");
                    }
                }
            }

            last_claimed_epochs.remove(&0);
        }

        let mut sorted_epochs: Vec<u64> = last_claimed_epochs.keys().copied().collect();
        sorted_epochs.sort_unstable();
        let mut epochs_pending = 0usize;
        let mut batches_attempted = 0usize;
        let mut batches_succeeded = 0usize;
        let mut batches_failed = 0usize;

        let mut idx = 0usize;
        while idx < sorted_epochs.len() {
            let epoch = sorted_epochs[idx];
            if epoch + 1 != current_epoch {
                epochs_pending += 1;
                let delegators_for_epoch = match last_claimed_epochs.get(&epoch) {
                    Some(delegators) => delegators.clone(),
                    None => {
                        idx += 1;
                        continue;
                    }
                };

                for batch in delegators_for_epoch.chunks(CLAIM_REWARDS_BATCH_SIZE) {
                    batches_attempted += 1;
                    let batch_started = Instant::now();
                    let result = self
                        .blockchain_manager
                        .batch_claim_delegator_rewards(
                            blockchain_id,
                            identity_id,
                            &[epoch + 1],
                            batch,
                        )
                        .await;

                    match result {
                        Ok(()) => {
                            batches_succeeded += 1;
                            observability::record_claim_rewards_batch(
                                blockchain_id.as_str(),
                                "ok",
                                batch.len(),
                                batch_started.elapsed(),
                            );
                            tracing::info!(
                                epoch = epoch + 1,
                                batch_size = batch.len(),
                                "Claimed delegator rewards batch"
                            );

                            let next_epoch = epoch + 1;
                            last_claimed_epochs
                                .entry(next_epoch)
                                .or_default()
                                .extend(batch.iter().copied());

                            if let Err(insert_pos) = sorted_epochs.binary_search(&next_epoch) {
                                sorted_epochs.insert(insert_pos, next_epoch);
                            }
                        }
                        Err(e) => {
                            batches_failed += 1;
                            observability::record_claim_rewards_batch(
                                blockchain_id.as_str(),
                                "error",
                                batch.len(),
                                batch_started.elapsed(),
                            );
                            tracing::warn!(
                                error = %e,
                                epoch = epoch + 1,
                                batch_size = batch.len(),
                                "Failed to claim delegator rewards batch"
                            );
                        }
                    }
                }
            }

            idx += 1;
        }

        observability::record_claim_rewards_cycle(
            blockchain_id.as_str(),
            "completed",
            cycle_started.elapsed(),
            delegator_count,
            epochs_pending,
            batches_attempted,
            batches_succeeded,
            batches_failed,
        );

        CLAIM_REWARDS_INTERVAL
    }
}
