mod handler;

use std::time::Duration;

pub(crate) use handler::{ClaimRewardsCommandData, ClaimRewardsCommandHandler};

/// Interval between claim rewards cycles (1 hour).
pub(crate) const CLAIM_REWARDS_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Maximum number of delegators claimed per transaction.
pub(crate) const CLAIM_REWARDS_BATCH_SIZE: usize = 10;
