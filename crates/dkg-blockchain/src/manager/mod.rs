mod cache;
mod claim_rewards;
mod contracts;
mod core;
mod events;
mod identity;
mod kc;
mod paranet;
mod random_sampling;
mod sharding;
mod staking;
use std::collections::HashMap;

use dkg_domain::BlockchainId;

use crate::{chains::evm::EvmChain, manager::cache::ParameterCache};
/// Manages multiple blockchain connections.
///
/// All EVM chains share the same implementation (`EvmChain`) with chain-specific
/// behavior determined by the `Blockchain` enum. This keeps the code simple while
/// still supporting chain-specific differences like:
/// - Native token decimals (NeuroWeb uses 12, others use 18)
/// - Native token ticker symbols
/// - Development chain flag
/// - EVM account mapping requirements (NeuroWeb)
pub struct BlockchainManager {
    blockchains: HashMap<BlockchainId, EvmChain>,
    parameter_cache: ParameterCache,
}
