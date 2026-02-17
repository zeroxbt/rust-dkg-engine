mod chains;
mod config;
mod config_error;
mod contract_events;
mod error;
mod error_classification;
mod manager;
mod multicall;
mod rpc_executor;
mod rpc_rate_limiter;
mod substrate;
mod utils;

use std::collections::HashMap;

use chains::evm::EvmChain;
pub use chains::evm::{ContractLog, ContractName, NodeInfo, ShardingTableNode};
pub use config::{
    BlockchainConfig, BlockchainConfigRaw, BlockchainManagerConfig, BlockchainManagerConfigRaw,
    BlockchainRaw,
};
pub use config_error::ConfigError;
pub use contract_events::{ContractEvent, decode_contract_event, monitored_contract_events};
pub use error::BlockchainError;
pub use multicall::{MulticallBatch, MulticallRequest, MulticallResult, encoders};
pub use rpc_rate_limiter::RpcRateLimiter;
pub use utils::{keccak256_encode_packed, parse_ether_to_u128, sha256_hex, to_hex_string};

// Re-export event types for use by consumers
// In alloy's sol! macro, events are nested under the contract module
pub type NewContractFilter = chains::evm::Hub::NewContract;
pub type ContractChangedFilter = chains::evm::Hub::ContractChanged;
pub type NewAssetStorageFilter = chains::evm::Hub::NewAssetStorage;
pub type AssetStorageChangedFilter = chains::evm::Hub::AssetStorageChanged;
pub type ParameterChangedFilter = chains::evm::ParametersStorage::ParameterChanged;
pub type KnowledgeCollectionCreatedFilter =
    chains::evm::KnowledgeCollectionStorage::KnowledgeCollectionCreated;
pub use alloy::{
    primitives::{Address, B256, B256 as H256, U256},
    rpc::types::Log,
};
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
