pub(crate) mod chains;
pub(crate) mod error;
mod error_utils;
mod gas;
mod manager;
pub(crate) mod multicall;
mod rpc_rate_limiter;
mod substrate;
pub(crate) mod utils;

use std::collections::HashMap;

use chains::evm::EvmChain;
pub(crate) use chains::evm::{
    ContractLog, ContractName, Hub, KnowledgeCollectionStorage, ParametersStorage,
};
pub(crate) use gas::GasConfig;
pub(crate) use rpc_rate_limiter::RpcRateLimiter;

use crate::config::ConfigError;

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
use serde::Deserialize;

use crate::managers::blockchain::{
    chains::evm::ShardingTableLib::NodeInfo, error::BlockchainError,
};
// Re-export shared domain types from crate::types
pub(crate) use crate::types::{BlockchainId, SignatureComponents};

/// Configuration for a blockchain network.
///
/// This struct contains all the settings needed to connect to and operate on
/// a specific blockchain network.
///
/// **Secret handling**: Private keys should be provided via environment variables
/// (resolved at config load time):
/// - `EVM_OPERATIONAL_WALLET_PRIVATE_KEY` - operational wallet private key (required)
/// - `EVM_MANAGEMENT_WALLET_PRIVATE_KEY` - management wallet private key (optional)
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct BlockchainConfig {
    /// Unique identifier for this blockchain instance.
    /// Format: "chaintype:chainid" (e.g., "hardhat:31337", "gnosis:100", "neuroweb:2043").
    /// The chain type determines the gas configuration and other chain-specific behavior.
    /// The chain ID is parsed from this field.
    blockchain_id: BlockchainId,

    /// Private key for the operational wallet (used for transactions).
    /// Set via EVM_OPERATIONAL_WALLET_PRIVATE_KEY env var or config file.
    #[serde(default)]
    evm_operational_wallet_private_key: Option<String>,

    /// Address for the operational wallet (20-byte EVM address).
    evm_operational_wallet_address: String,

    /// Address for the management wallet (20-byte EVM address, used for admin operations).
    evm_management_wallet_address: String,

    /// Private key for the management wallet (optional, only needed for staking/admin ops).
    /// Set via EVM_MANAGEMENT_WALLET_PRIVATE_KEY env var or config file.
    #[serde(default)]
    evm_management_wallet_private_key: Option<String>,

    /// Hub contract address for this network.
    hub_contract_address: String,

    /// RPC endpoints for EVM JSON-RPC calls (supports HTTP and WebSocket).
    /// Multiple endpoints enable fallback if primary fails.
    rpc_endpoints: Vec<String>,

    /// Node name used in profile creation.
    /// This identifies the node on the network.
    node_name: String,

    /// URL for gas price oracle (optional).
    /// Used by Gnosis to fetch current gas prices from Blockscout.
    #[serde(default)]
    gas_price_oracle_url: Option<String>,

    /// Operator fee percentage for delegators (0-100).
    /// This is the percentage of rewards kept by the node operator.
    /// Only required on first profile creation.
    #[serde(default)]
    operator_fee: Option<u8>,

    /// Substrate RPC endpoints for parachain operations (NeuroWeb only).
    /// Used for EVM account mapping validation.
    /// Supports WebSocket (wss://) and HTTP (https://) endpoints.
    #[serde(default)]
    substrate_rpc_endpoints: Option<Vec<String>>,

    /// Maximum RPC requests per second (optional rate limiting).
    /// Set this based on your RPC provider's rate limits.
    /// Common values: 25 (free tier), 50-100 (paid tier), None (unlimited).
    #[serde(default)]
    max_rpc_requests_per_second: Option<u32>,
}

impl BlockchainConfig {
    pub(crate) fn blockchain_id(&self) -> &BlockchainId {
        &self.blockchain_id
    }

    /// Returns the operational wallet private key.
    /// This is guaranteed to be set after config initialization.
    pub(crate) fn evm_operational_wallet_private_key(&self) -> &str {
        self.evm_operational_wallet_private_key
            .as_ref()
            .expect("evm_operational_wallet_private_key should be set during config initialization")
    }

    pub(crate) fn evm_operational_wallet_address(&self) -> &str {
        &self.evm_operational_wallet_address
    }

    pub(crate) fn evm_management_wallet_address(&self) -> &str {
        &self.evm_management_wallet_address
    }

    /// Returns the management wallet private key if available.
    pub(crate) fn evm_management_wallet_private_key(&self) -> Option<&str> {
        self.evm_management_wallet_private_key.as_deref()
    }

    /// Sets the operational wallet private key (called during secret resolution).
    pub(crate) fn set_operational_wallet_private_key(&mut self, key: String) {
        self.evm_operational_wallet_private_key = Some(key);
    }

    /// Sets the management wallet private key (called during secret resolution).
    pub(crate) fn set_management_wallet_private_key(&mut self, key: String) {
        self.evm_management_wallet_private_key = Some(key);
    }

    /// Ensures the operational wallet private key is set.
    pub(crate) fn ensure_operational_wallet_private_key(&self) -> Result<(), ConfigError> {
        if self.evm_operational_wallet_private_key.is_none() {
            return Err(ConfigError::MissingSecret(
                "EVM_OPERATIONAL_WALLET_PRIVATE_KEY env var or evm_operational_wallet_private_key config required".to_string()
            ));
        }
        Ok(())
    }

    pub(crate) fn rpc_endpoints(&self) -> &Vec<String> {
        &self.rpc_endpoints
    }

    pub(crate) fn node_name(&self) -> &str {
        &self.node_name
    }

    pub(crate) fn gas_price_oracle_url(&self) -> Option<&str> {
        self.gas_price_oracle_url.as_deref()
    }

    pub(crate) fn operator_fee(&self) -> Option<u8> {
        self.operator_fee
    }

    pub(crate) fn substrate_rpc_endpoints(&self) -> Option<&Vec<String>> {
        self.substrate_rpc_endpoints.as_ref()
    }

    pub(crate) fn max_rpc_requests_per_second(&self) -> Option<u32> {
        self.max_rpc_requests_per_second
    }
}

/// Supported blockchain types for configuration.
///
/// The enum variant determines chain-specific behavior (gas config, token decimals, etc.)
/// and is used for TOML config structure (e.g., `[managers.blockchain.Hardhat]`).
/// The `blockchain_id` field is a protocol identifier for network communication.
#[derive(Debug, Deserialize, Clone)]
pub(crate) enum Blockchain {
    /// Local Hardhat network for development/testing
    Hardhat(BlockchainConfig),
    /// Gnosis Chain (formerly xDai) - mainnet chain_id: 100, testnet (Chiado): 10200
    Gnosis(BlockchainConfig),
    /// NeuroWeb (OT Parachain) - mainnet chain_id: 2043, testnet: 20430
    NeuroWeb(BlockchainConfig),
    /// Base (Coinbase L2) - mainnet chain_id: 8453, testnet (Sepolia): 84532
    Base(BlockchainConfig),
}

impl Blockchain {
    pub(crate) fn get_config(&self) -> &BlockchainConfig {
        match self {
            Blockchain::Hardhat(config)
            | Blockchain::Gnosis(config)
            | Blockchain::NeuroWeb(config)
            | Blockchain::Base(config) => config,
        }
    }

    pub(crate) fn get_config_mut(&mut self) -> &mut BlockchainConfig {
        match self {
            Blockchain::Hardhat(config)
            | Blockchain::Gnosis(config)
            | Blockchain::NeuroWeb(config)
            | Blockchain::Base(config) => config,
        }
    }

    /// Creates the appropriate gas configuration based on blockchain type.
    pub(crate) fn gas_config(&self) -> GasConfig {
        let oracle_url = self
            .get_config()
            .gas_price_oracle_url()
            .map(|s| s.to_string());
        match self {
            Blockchain::Hardhat(_) => GasConfig::hardhat(),
            Blockchain::Gnosis(_) => GasConfig::gnosis(oracle_url),
            Blockchain::NeuroWeb(_) => GasConfig::neuroweb(oracle_url),
            Blockchain::Base(_) => GasConfig::base(oracle_url),
        }
    }

    /// Native token decimal places.
    /// NeuroWeb uses 12 decimals (NEURO), others use standard 18 (ETH/xDAI).
    pub(crate) fn native_token_decimals(&self) -> u8 {
        match self {
            Blockchain::NeuroWeb(_) => 12,
            _ => 18,
        }
    }

    /// Native token ticker symbol.
    pub(crate) fn native_token_ticker(&self) -> &'static str {
        match self {
            Blockchain::Hardhat(_) => "ETH",
            Blockchain::Gnosis(_) => "xDAI",
            Blockchain::NeuroWeb(_) => "NEURO",
            Blockchain::Base(_) => "ETH",
        }
    }

    /// Whether this chain requires EVM account mapping validation.
    ///
    /// On NeuroWeb, EVM addresses must be mapped to Substrate accounts.
    pub(crate) fn requires_evm_account_mapping(&self) -> bool {
        matches!(self, Blockchain::NeuroWeb(_))
    }
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct BlockchainManagerConfig(pub Vec<Blockchain>);

impl BlockchainManagerConfig {
    /// Returns mutable references to all blockchain configs for secret resolution.
    pub(crate) fn configs_mut(&mut self) -> impl Iterator<Item = &mut BlockchainConfig> {
        self.0.iter_mut().map(|b| b.get_config_mut())
    }
}

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

impl BlockchainManager {
    pub(crate) async fn connect(config: &BlockchainManagerConfig) -> Result<Self, BlockchainError> {
        let mut blockchains = HashMap::new();

        for blockchain in config.0.iter() {
            let blockchain_config = blockchain.get_config();
            let blockchain_id = blockchain_config.blockchain_id().clone();

            // Validate blockchain_id format (must contain chain_id)
            if blockchain_id.chain_id().is_none() {
                return Err(BlockchainError::InvalidBlockchainId {
                    blockchain_id: blockchain_id.to_string(),
                });
            }

            // Check for duplicate blockchain IDs
            if blockchains.contains_key(&blockchain_id) {
                return Err(BlockchainError::DuplicateBlockchainId {
                    blockchain_id: blockchain_id.to_string(),
                });
            }

            let evm_chain = EvmChain::new(
                blockchain_config.clone(),
                blockchain.gas_config(),
                blockchain.native_token_decimals(),
                blockchain.native_token_ticker(),
                blockchain.requires_evm_account_mapping(),
            )
            .await?;
            blockchains.insert(blockchain_id, evm_chain);
        }

        Ok(Self { blockchains })
    }

    fn chain(&self, blockchain: &BlockchainId) -> Result<&EvmChain, BlockchainError> {
        self.blockchains
            .get(blockchain)
            .ok_or_else(|| BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            })
    }

    pub(crate) fn get_blockchain_ids(&self) -> Vec<&BlockchainId> {
        self.blockchains.keys().collect()
    }
}

#[cfg(test)]
mod tests {
    use chains::evm::format_balance;

    use super::*;

    fn config_for_chain(chain_type: &str, chain_id: u64) -> BlockchainConfig {
        BlockchainConfig {
            blockchain_id: BlockchainId::from(format!("{}:{}", chain_type, chain_id)),
            evm_operational_wallet_private_key: None,
            evm_operational_wallet_address: String::new(),
            evm_management_wallet_address: String::new(),
            evm_management_wallet_private_key: None,
            hub_contract_address: String::new(),
            rpc_endpoints: vec![],
            node_name: String::new(),
            gas_price_oracle_url: None,
            operator_fee: None,
            substrate_rpc_endpoints: None,
            max_rpc_requests_per_second: None,
        }
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

    #[test]
    fn test_format_balance_18_decimals() {
        // 1 ETH = 10^18 wei
        let one_eth = U256::from(10u64).pow(U256::from(18u64));
        assert_eq!(format_balance(one_eth, 18), "1");

        // 1.5 ETH
        let one_point_five = one_eth + one_eth / U256::from(2u64);
        assert_eq!(format_balance(one_point_five, 18), "1.5");

        // 0 ETH
        assert_eq!(format_balance(U256::ZERO, 18), "0");

        // 0.001 ETH = 10^15 wei
        let small = U256::from(10u64).pow(U256::from(15u64));
        assert_eq!(format_balance(small, 18), "0.001");
    }

    #[test]
    fn test_format_balance_12_decimals() {
        // 1 NEURO = 10^12 wei (12 decimals)
        let one_neuro = U256::from(10u64).pow(U256::from(12u64));
        assert_eq!(format_balance(one_neuro, 12), "1");

        // 1.5 NEURO
        let one_point_five = one_neuro + one_neuro / U256::from(2u64);
        assert_eq!(format_balance(one_point_five, 12), "1.5");

        // 0.001 NEURO = 10^9 wei
        let small = U256::from(10u64).pow(U256::from(9u64));
        assert_eq!(format_balance(small, 12), "0.001");
    }
}
