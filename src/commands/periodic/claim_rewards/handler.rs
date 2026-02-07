use std::{collections::HashMap, sync::Arc};

use alloy::primitives::Address;
use futures::stream::{FuturesUnordered, StreamExt};

use super::{CLAIM_REWARDS_BATCH_SIZE, CLAIM_REWARDS_INTERVAL};
use crate::{
    commands::{executor::CommandExecutionResult, registry::CommandHandler},
    context::Context,
    managers::blockchain::BlockchainManager,
    types::BlockchainId,
};

#[derive(Clone)]
pub(crate) struct ClaimRewardsCommandData {
    pub blockchain_id: BlockchainId,
}

impl ClaimRewardsCommandData {
    pub(crate) fn new(blockchain_id: BlockchainId) -> Self {
        Self { blockchain_id }
    }
}

pub(crate) struct ClaimRewardsCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
}

impl ClaimRewardsCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
        }
    }
}

impl CommandHandler<ClaimRewardsCommandData> for ClaimRewardsCommandHandler {
    #[tracing::instrument(
        name = "periodic.claim_rewards",
        skip(self, data),
        fields(blockchain_id = %data.blockchain_id)
    )]
    async fn execute(&self, data: &ClaimRewardsCommandData) -> CommandExecutionResult {
        let identity_id = self.blockchain_manager.identity_id(&data.blockchain_id);

        let delegators = match self
            .blockchain_manager
            .get_delegators(&data.blockchain_id, identity_id)
            .await
        {
            Ok(delegators) => delegators,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to fetch delegators");
                return CommandExecutionResult::Repeat {
                    delay: CLAIM_REWARDS_INTERVAL,
                };
            }
        };

        if delegators.is_empty() {
            tracing::debug!("No delegators found");
            return CommandExecutionResult::Repeat {
                delay: CLAIM_REWARDS_INTERVAL,
            };
        }

        let current_epoch = match self
            .blockchain_manager
            .get_current_epoch(&data.blockchain_id)
            .await
        {
            Ok(epoch) => epoch,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get current epoch");
                return CommandExecutionResult::Repeat {
                    delay: CLAIM_REWARDS_INTERVAL,
                };
            }
        };

        let mut last_claimed_epochs: HashMap<u64, Vec<Address>> = HashMap::new();
        let mut last_claimed_futures = FuturesUnordered::new();

        for delegator in delegators {
            let blockchain_id = data.blockchain_id.clone();
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
                let blockchain_id = data.blockchain_id.clone();
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

        let mut idx = 0usize;
        while idx < sorted_epochs.len() {
            let epoch = sorted_epochs[idx];
            if epoch + 1 != current_epoch {
                let delegators_for_epoch = match last_claimed_epochs.get(&epoch) {
                    Some(delegators) => delegators.clone(),
                    None => {
                        idx += 1;
                        continue;
                    }
                };

                for batch in delegators_for_epoch.chunks(CLAIM_REWARDS_BATCH_SIZE) {
                    let result = self
                        .blockchain_manager
                        .batch_claim_delegator_rewards(
                            &data.blockchain_id,
                            identity_id,
                            &[epoch + 1],
                            batch,
                        )
                        .await;

                    match result {
                        Ok(()) => {
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

        CommandExecutionResult::Repeat {
            delay: CLAIM_REWARDS_INTERVAL,
        }
    }
}
