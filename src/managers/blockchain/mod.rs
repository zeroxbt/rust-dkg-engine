pub(crate) mod chains;
mod config;
pub(crate) mod error;
mod error_classification;
mod manager;
pub(crate) mod multicall;
mod rpc_executor;
mod rpc_rate_limiter;
mod substrate;
pub(crate) mod utils;

use std::collections::HashMap;

use chains::evm::EvmChain;
pub(crate) use chains::evm::{
    ContractLog, ContractName, Hub, KnowledgeCollectionStorage, ParametersStorage,
};
pub(crate) use config::{
    BlockchainConfig, BlockchainConfigRaw, BlockchainManagerConfig, BlockchainManagerConfigRaw,
    BlockchainRaw,
};
pub(crate) use rpc_rate_limiter::RpcRateLimiter;

// Re-export event types for use by consumers
// In alloy's sol! macro, events are nested under the contract module
pub(crate) type NewContractFilter = Hub::NewContract;
pub(crate) type ContractChangedFilter = Hub::ContractChanged;
pub(crate) type NewAssetStorageFilter = Hub::NewAssetStorage;
pub(crate) type AssetStorageChangedFilter = Hub::AssetStorageChanged;
pub(crate) type ParameterChangedFilter = ParametersStorage::ParameterChanged;
pub(crate) type KnowledgeCollectionCreatedFilter =
    KnowledgeCollectionStorage::KnowledgeCollectionCreated;
pub(crate) use alloy::primitives::{Address, B256 as H256, U256};

// Re-export shared domain types from crate::types
pub(crate) use crate::types::BlockchainId;

/// Manages multiple blockchain connections.
///
/// All EVM chains share the same implementation (`EvmChain`) with chain-specific
/// behavior determined by the `Blockchain` enum. This keeps the code simple while
/// still supporting chain-specific differences like:
/// - Native token decimals (NeuroWeb uses 12, others use 18)
/// - Native token ticker symbols
/// - Development chain flag
/// - EVM account mapping requirements (NeuroWeb)
pub(crate) struct BlockchainManager {
    blockchains: HashMap<BlockchainId, EvmChain>,
}
