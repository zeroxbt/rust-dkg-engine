//! Proving Service
//!
//! Periodic task that submits random sampling proofs to the blockchain.
//! Runs every 5 minutes per blockchain to:
//! 1. Check if node is part of the shard
//! 2. Get/create challenge from RandomSampling contract
//! 3. Fetch assertion (locally or from network)
//! 4. Calculate Merkle proof for the challenged chunk
//! 5. Submit proof to blockchain

mod task;

use std::time::Duration;

pub(crate) use task::ProvingTask;

/// Interval between proving cycles (5 minutes)
pub(crate) const PROVING_PERIOD: Duration = Duration::from_secs(300);

/// Buffer time before checking finalization (60 seconds)
/// Helps ensure blockchain state is stable after proof submission.
pub(crate) const REORG_BUFFER: Duration = Duration::from_secs(60);
