pub mod blockchains;
pub mod error;
pub mod error_utils;
pub mod gas;
pub mod utils;

use std::collections::HashMap;

use blockchains::evm_chain::EvmChain;
pub use blockchains::{
    blockchain_creator::{Hub, KnowledgeCollectionStorage, ParametersStorage},
    evm_chain::{ContractLog, ContractName},
};
pub use gas::{GasConfig, GasOracleError};

// Re-export event types for use by consumers
// In alloy's sol! macro, events are nested under the contract module
pub type NewContractFilter = Hub::NewContract;
pub type ContractChangedFilter = Hub::ContractChanged;
pub type NewAssetStorageFilter = Hub::NewAssetStorage;
pub type AssetStorageChangedFilter = Hub::AssetStorageChanged;
pub type ParameterChangedFilter = ParametersStorage::ParameterChanged;
pub type KnowledgeCollectionCreatedFilter = KnowledgeCollectionStorage::KnowledgeCollectionCreated;
pub use alloy::primitives::{Address, B256 as H256, U256};
use serde::{Deserialize, Serialize};

use crate::{
    blockchains::blockchain_creator::sharding_table::ShardingTableLib::NodeInfo,
    error::BlockchainError,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct BlockchainId(String);

impl BlockchainId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BlockchainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for BlockchainId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for BlockchainId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// ECDSA signature components for EVM transactions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignatureComponents {
    pub v: u8,
    pub r: String,
    pub s: String,
    pub vs: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BlockchainConfig {
    #[serde(default)]
    blockchain_id: Option<BlockchainId>,
    chain_id: u64,
    evm_operational_wallet_private_key: String,
    evm_operational_wallet_public_key: String,
    evm_management_wallet_public_key: String,
    evm_management_wallet_private_key: Option<String>,
    hub_contract_address: String,
    rpc_endpoints: Vec<String>,
    shares_token_name: String,
    shares_token_symbol: String,
    /// URL for gas price oracle (optional).
    #[serde(default)]
    gas_price_oracle_url: Option<String>,
}

impl BlockchainConfig {
    pub fn blockchain_id(&self) -> &BlockchainId {
        self.blockchain_id
            .as_ref()
            .expect("blockchain_id must be set before use")
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub fn evm_operational_wallet_private_key(&self) -> &str {
        &self.evm_operational_wallet_private_key
    }

    pub fn evm_operational_wallet_public_key(&self) -> &str {
        &self.evm_operational_wallet_public_key
    }

    pub fn evm_management_wallet_public_key(&self) -> &str {
        &self.evm_management_wallet_public_key
    }

    pub fn evm_management_wallet_private_key(&self) -> Option<&String> {
        self.evm_management_wallet_private_key.as_ref()
    }

    pub fn hub_contract_address(&self) -> &str {
        &self.hub_contract_address
    }

    pub fn rpc_endpoints(&self) -> &Vec<String> {
        &self.rpc_endpoints
    }

    pub fn shares_token_name(&self) -> &str {
        &self.shares_token_name
    }

    pub fn shares_token_symbol(&self) -> &str {
        &self.shares_token_symbol
    }

    pub fn gas_price_oracle_url(&self) -> Option<&str> {
        self.gas_price_oracle_url.as_deref()
    }
}

/// Supported blockchain types.
///
/// Each variant represents a different blockchain network with its own
/// gas price configuration and RPC endpoints.
#[derive(Debug, Deserialize, Clone)]
pub enum Blockchain {
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
    pub fn get_config(&self) -> &BlockchainConfig {
        match self {
            Blockchain::Hardhat(config) => config,
            Blockchain::Gnosis(config) => config,
            Blockchain::NeuroWeb(config) => config,
            Blockchain::Base(config) => config,
        }
    }

    /// Returns the default blockchain ID prefix for this blockchain type.
    pub fn default_id_prefix(&self) -> &'static str {
        match self {
            Blockchain::Hardhat(_) => "hardhat",
            Blockchain::Gnosis(_) => "gnosis",
            Blockchain::NeuroWeb(_) => "neuroweb",
            Blockchain::Base(_) => "base",
        }
    }

    /// Creates the appropriate gas configuration for this blockchain.
    pub fn gas_config(&self) -> GasConfig {
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
}

#[derive(Debug, Deserialize, Clone)]
pub struct BlockchainManagerConfig(pub Vec<Blockchain>);

pub struct BlockchainManager {
    blockchains: HashMap<BlockchainId, EvmChain>,
}

impl BlockchainManager {
    pub async fn new(config: &BlockchainManagerConfig) -> Result<Self, BlockchainError> {
        let mut blockchains = HashMap::new();
        for blockchain in config.0.iter() {
            let blockchain_config = blockchain.get_config();
            let id_prefix = blockchain.default_id_prefix();
            let gas_config = blockchain.gas_config();

            let mut config = blockchain_config.clone();
            let blockchain_id = config.blockchain_id.clone().unwrap_or_else(|| {
                BlockchainId::new(format!("{}:{}", id_prefix, config.chain_id()))
            });
            config.blockchain_id = Some(blockchain_id.clone());

            let evm_chain = blockchains::evm_chain::EvmChain::new(config, gas_config).await?;

            blockchains.insert(blockchain_id, evm_chain);
        }

        Ok(Self { blockchains })
    }

    pub fn get_blockchain_ids(&self) -> Vec<&BlockchainId> {
        self.blockchains.keys().collect()
    }

    pub fn get_blockchain_config(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<&BlockchainConfig, BlockchainError> {
        self.blockchains
            .get(blockchain)
            .map(|b| b.config())
            .ok_or_else(|| BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            })
    }

    pub async fn identity_id_exists(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        Ok(blockchain_impl.identity_id_exists().await)
    }

    pub async fn get_identity_id(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<Option<u128>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        Ok(blockchain_impl.get_identity_id().await)
    }

    pub async fn initialize_identities(&mut self, peer_id: &str) -> Result<(), BlockchainError> {
        for blockchain in self.blockchains.values_mut() {
            blockchain.initialize_identity(peer_id).await?;
        }

        Ok(())
    }

    pub async fn get_event_logs(
        &self,
        blockchain: &BlockchainId,
        contract_name: &ContractName,
        event_signatures: &[H256],
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<ContractLog>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl
            .get_event_logs(contract_name, event_signatures, from_block, current_block)
            .await
    }

    pub async fn get_block_number(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u64, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl.get_block_number().await
    }

    pub async fn get_sharding_table_head(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u128, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;

        blockchain_impl.get_sharding_table_head().await
    }

    pub async fn get_sharding_table_length(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u128, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;

        blockchain_impl.get_sharding_table_length().await
    }

    pub async fn get_sharding_table_page(
        &self,
        blockchain: &BlockchainId,
        starting_identity_id: u128,
        nodes_num: u128,
    ) -> Result<Vec<NodeInfo>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;

        blockchain_impl
            .get_sharding_table_page(starting_identity_id, nodes_num)
            .await
    }

    pub async fn re_initialize_contract(
        &self,
        blockchain: &BlockchainId,
        contract_name: String,
        contract_address: Address,
    ) -> Result<(), BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl
            .re_initialize_contract(contract_name, contract_address)
            .await
    }

    // Note: get_assertion_id_by_index removed - ContentAssetStorage not currently in use

    pub async fn sign_message(
        &self,
        blockchain: &BlockchainId,
        message_hash: &str,
    ) -> Result<SignatureComponents, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl.sign_message(message_hash).await
    }

    /// Sets the stake for a node's identity (dev environment only).
    /// stake_wei should be the stake amount in wei (e.g., 50_000 * 10^18 for 50,000 tokens).
    pub async fn set_stake(
        &self,
        blockchain: &BlockchainId,
        stake_wei: u128,
    ) -> Result<(), BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl.set_stake(stake_wei).await
    }

    /// Sets the ask price for a node's identity (dev environment only).
    /// ask_wei should be the ask amount in wei (e.g., 0.2 * 10^18 for 0.2 tokens).
    pub async fn set_ask(
        &self,
        blockchain: &BlockchainId,
        ask_wei: u128,
    ) -> Result<(), BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl.set_ask(ask_wei).await
    }

    /// Get the current gas price for a blockchain.
    pub async fn get_gas_price(&self, blockchain: &BlockchainId) -> Result<U256, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        Ok(blockchain_impl.get_gas_price().await)
    }

    /// Get the gas configuration for a blockchain.
    pub fn get_gas_config(&self, blockchain: &BlockchainId) -> Result<&GasConfig, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        Ok(blockchain_impl.gas_config())
    }
}
