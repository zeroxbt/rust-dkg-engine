pub mod chains;
mod config;
mod config_error;
pub mod error;
mod error_classification;
mod manager;
pub mod multicall;
mod rpc_executor;
mod rpc_rate_limiter;
mod substrate;
pub mod utils;

use std::collections::HashMap;

use chains::evm::EvmChain;
pub use chains::evm::{
    ContractLog, ContractName, Hub, KnowledgeCollectionStorage, ParametersStorage,
};
pub use config::{
    BlockchainConfig, BlockchainConfigRaw, BlockchainManagerConfig, BlockchainManagerConfigRaw,
    BlockchainRaw,
};
pub use config_error::ConfigError;
pub use rpc_rate_limiter::RpcRateLimiter;

// Re-export event types for use by consumers
// In alloy's sol! macro, events are nested under the contract module
pub type NewContractFilter = Hub::NewContract;
pub type ContractChangedFilter = Hub::ContractChanged;
pub type NewAssetStorageFilter = Hub::NewAssetStorage;
pub type AssetStorageChangedFilter = Hub::AssetStorageChanged;
pub type ParameterChangedFilter = ParametersStorage::ParameterChanged;
pub type KnowledgeCollectionCreatedFilter = KnowledgeCollectionStorage::KnowledgeCollectionCreated;
pub use alloy::primitives::{Address, B256 as H256, U256};

// Re-export shared domain types from crate::types
pub use dkg_domain::BlockchainId;

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
}
