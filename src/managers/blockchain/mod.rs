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
pub(crate) use config::{BlockchainConfig, BlockchainManagerConfig};
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
pub(crate) use alloy::primitives::{Address, B256, B256 as H256, U256};

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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::managers::blockchain::config::Blockchain;

    fn config_for_chain(chain_type: &str, chain_id: u64) -> BlockchainConfig {
        BlockchainConfig::test_config(chain_type, chain_id)
    }

    #[test]
    fn test_blockchain_id_parsing() {
        let id = BlockchainId::from("otp:2043");
        assert_eq!(id.chain_id(), Some(2043));

        let id = BlockchainId::from("hardhat1:31337");
        assert_eq!(id.chain_id(), Some(31337));

        // Missing chain_id
        let id = BlockchainId::from("hardhat1");
        assert_eq!(id.chain_id(), None);
    }

    #[test]
    fn test_blockchain_native_token_decimals() {
        // Using protocol-correct prefixes
        assert_eq!(
            Blockchain::Hardhat(config_for_chain("hardhat1", 31337)).native_token_decimals(),
            18
        );
        assert_eq!(
            Blockchain::Gnosis(config_for_chain("gnosis", 100)).native_token_decimals(),
            18
        );
        assert_eq!(
            Blockchain::NeuroWeb(config_for_chain("otp", 2043)).native_token_decimals(),
            12
        );
        assert_eq!(
            Blockchain::Base(config_for_chain("base", 8453)).native_token_decimals(),
            18
        );
    }

    #[test]
    fn test_blockchain_native_token_ticker() {
        assert_eq!(
            Blockchain::Hardhat(config_for_chain("hardhat1", 31337)).native_token_ticker(),
            "ETH"
        );
        assert_eq!(
            Blockchain::Gnosis(config_for_chain("gnosis", 100)).native_token_ticker(),
            "xDAI"
        );
        assert_eq!(
            Blockchain::NeuroWeb(config_for_chain("otp", 2043)).native_token_ticker(),
            "NEURO"
        );
        assert_eq!(
            Blockchain::Base(config_for_chain("base", 8453)).native_token_ticker(),
            "ETH"
        );
    }

    #[test]
    fn test_blockchain_requires_evm_account_mapping() {
        assert!(
            !Blockchain::Hardhat(config_for_chain("hardhat1", 31337))
                .requires_evm_account_mapping()
        );
        assert!(
            !Blockchain::Gnosis(config_for_chain("gnosis", 100)).requires_evm_account_mapping()
        );
        assert!(Blockchain::NeuroWeb(config_for_chain("otp", 2043)).requires_evm_account_mapping());
        assert!(!Blockchain::Base(config_for_chain("base", 8453)).requires_evm_account_mapping());
    }
}
