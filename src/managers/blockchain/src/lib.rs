pub mod blockchains;
pub mod error;
pub mod error_utils;
pub mod gas;
pub mod substrate;
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
pub use alloy::primitives::{Address, B256, B256 as H256, U256};
use serde::{Deserialize, Serialize};

/// Access policy for paranet nodes (matches on-chain enum).
/// OPEN = 0: Any node can participate
/// PERMISSIONED = 1: Only approved nodes can participate
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AccessPolicy {
    Open = 0,
    Permissioned = 1,
}

impl From<u8> for AccessPolicy {
    fn from(value: u8) -> Self {
        match value {
            1 => AccessPolicy::Permissioned,
            _ => AccessPolicy::Open,
        }
    }
}

/// A node permitted to participate in a permissioned paranet.
#[derive(Debug, Clone)]
pub struct PermissionedNode {
    /// The node's identity ID on-chain
    pub identity_id: u128,
    /// The node's peer ID as bytes
    pub node_id: Vec<u8>,
}

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

    /// Parse the prefix from the blockchain ID.
    /// E.g., "hardhat1:31337" -> "hardhat1", "otp:2043" -> "otp"
    pub fn prefix(&self) -> &str {
        self.0.split(':').next().unwrap_or(&self.0)
    }

    /// Parse the chain ID from the blockchain ID.
    /// E.g., "hardhat1:31337" -> 31337, "otp:2043" -> 2043
    /// Returns None if the chain ID is missing or not a valid number.
    pub fn chain_id(&self) -> Option<u64> {
        self.0.split(':').nth(1).and_then(|s| s.parse().ok())
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

/// Configuration for a blockchain network.
///
/// This struct contains all the settings needed to connect to and operate on
/// a specific blockchain network.
#[derive(Debug, Clone, Deserialize)]
pub struct BlockchainConfig {
    /// Unique identifier for this blockchain instance.
    /// Format: "chaintype:chainid" (e.g., "hardhat:31337", "gnosis:100", "neuroweb:2043").
    /// The chain type determines the gas configuration and other chain-specific behavior.
    /// The chain ID is parsed from this field.
    blockchain_id: BlockchainId,

    /// Private key for the operational wallet (used for transactions).
    evm_operational_wallet_private_key: String,

    /// Address for the operational wallet (20-byte EVM address).
    evm_operational_wallet_address: String,

    /// Address for the management wallet (20-byte EVM address, used for admin operations).
    evm_management_wallet_address: String,

    /// Private key for the management wallet (optional, only needed for staking/admin ops).
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

    /// Initial stake amount in whole tokens (e.g., 50000 for 50,000 TRAC).
    /// Only used in development for automatic staking.
    #[serde(default)]
    initial_stake_amount: Option<u64>,

    /// Initial ask price in tokens (e.g., 0.2 TRAC per unit).
    /// Only used in development for automatic ask setting.
    #[serde(default)]
    initial_ask_amount: Option<f64>,

    /// Substrate RPC endpoints for parachain operations (NeuroWeb only).
    /// Used for EVM account mapping validation.
    /// Supports WebSocket (wss://) and HTTP (https://) endpoints.
    #[serde(default)]
    substrate_rpc_endpoints: Option<Vec<String>>,
}

impl BlockchainConfig {
    pub fn blockchain_id(&self) -> &BlockchainId {
        &self.blockchain_id
    }

    /// Returns the chain ID parsed from the blockchain_id.
    /// Panics if the blockchain_id is malformed (should be validated at config load time).
    pub fn chain_id(&self) -> u64 {
        self.blockchain_id
            .chain_id()
            .expect("blockchain_id should contain valid chain_id (format: 'type:chainid')")
    }

    pub fn evm_operational_wallet_private_key(&self) -> &str {
        &self.evm_operational_wallet_private_key
    }

    pub fn evm_operational_wallet_address(&self) -> &str {
        &self.evm_operational_wallet_address
    }

    pub fn evm_management_wallet_address(&self) -> &str {
        &self.evm_management_wallet_address
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

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn gas_price_oracle_url(&self) -> Option<&str> {
        self.gas_price_oracle_url.as_deref()
    }

    pub fn operator_fee(&self) -> Option<u8> {
        self.operator_fee
    }

    pub fn initial_stake_amount(&self) -> Option<u64> {
        self.initial_stake_amount
    }

    pub fn initial_ask_amount(&self) -> Option<f64> {
        self.initial_ask_amount
    }

    pub fn substrate_rpc_endpoints(&self) -> Option<&Vec<String>> {
        self.substrate_rpc_endpoints.as_ref()
    }
}

/// Supported blockchain types for configuration.
///
/// The enum variant determines chain-specific behavior (gas config, token decimals, etc.)
/// and is used for TOML config structure (e.g., `[managers.blockchain.Hardhat]`).
/// The `blockchain_id` field is a protocol identifier for network communication.
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
            Blockchain::Hardhat(config)
            | Blockchain::Gnosis(config)
            | Blockchain::NeuroWeb(config)
            | Blockchain::Base(config) => config,
        }
    }

    /// Creates the appropriate gas configuration based on blockchain type.
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

    /// Native token decimal places.
    /// NeuroWeb uses 12 decimals (NEURO), others use standard 18 (ETH/xDAI).
    pub fn native_token_decimals(&self) -> u8 {
        match self {
            Blockchain::NeuroWeb(_) => 12,
            _ => 18,
        }
    }

    /// Native token ticker symbol.
    pub fn native_token_ticker(&self) -> &'static str {
        match self {
            Blockchain::Hardhat(_) => "ETH",
            Blockchain::Gnosis(_) => "xDAI",
            Blockchain::NeuroWeb(_) => "NEURO",
            Blockchain::Base(_) => "ETH",
        }
    }

    /// Whether this is a development/test chain.
    pub fn is_development_chain(&self) -> bool {
        matches!(self, Blockchain::Hardhat(_))
    }

    /// Whether this chain requires EVM account mapping validation.
    ///
    /// On NeuroWeb, EVM addresses must be mapped to Substrate accounts.
    pub fn requires_evm_account_mapping(&self) -> bool {
        matches!(self, Blockchain::NeuroWeb(_))
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct BlockchainManagerConfig(pub Vec<Blockchain>);

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

impl BlockchainManager {
    pub async fn connect(config: &BlockchainManagerConfig) -> Result<Self, BlockchainError> {
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
                blockchain.is_development_chain(),
                blockchain.requires_evm_account_mapping(),
            )
            .await?;
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

    /// Get the sender address of a transaction by its hash.
    pub async fn get_transaction_sender(
        &self,
        blockchain: &BlockchainId,
        tx_hash: H256,
    ) -> Result<Option<Address>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl.get_transaction_sender(tx_hash).await
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

    pub async fn get_minimum_required_signatures(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u64, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;

        blockchain_impl.get_minimum_required_signatures().await
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

    /// Get the native token decimals for a blockchain.
    pub fn get_native_token_decimals(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u8, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        Ok(blockchain_impl.native_token_decimals())
    }

    /// Get the native token ticker for a blockchain.
    pub fn get_native_token_ticker(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<&'static str, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        Ok(blockchain_impl.native_token_ticker())
    }

    /// Check if a blockchain is a development chain.
    pub fn is_development_chain(&self, blockchain: &BlockchainId) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        Ok(blockchain_impl.is_development_chain())
    }

    /// Get the native token balance for an address, formatted with correct decimals.
    pub async fn get_native_token_balance(
        &self,
        blockchain: &BlockchainId,
        address: Address,
    ) -> Result<String, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl.get_native_token_balance(address).await
    }

    /// Check if a knowledge collection exists on-chain.
    ///
    /// Returns the publisher address if the collection exists, or None if it doesn't.
    /// This is used to validate UALs before sending get requests.
    pub async fn get_knowledge_collection_publisher(
        &self,
        blockchain: &BlockchainId,
        knowledge_collection_id: u128,
    ) -> Result<Option<Address>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl
            .get_knowledge_collection_publisher(knowledge_collection_id)
            .await
    }

    /// Get the range of knowledge assets (token IDs) for a knowledge collection.
    ///
    /// Returns (start_token_id, end_token_id, burned_token_ids) or None if the collection
    /// doesn't exist.
    pub async fn get_knowledge_assets_range(
        &self,
        blockchain: &BlockchainId,
        knowledge_collection_id: u128,
    ) -> Result<Option<(u64, u64, Vec<u64>)>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl
            .get_knowledge_assets_range(knowledge_collection_id)
            .await
    }

    /// Get the latest merkle root for a knowledge collection.
    ///
    /// Returns the merkle root as a hex string (with 0x prefix), or None if the collection
    /// doesn't exist.
    pub async fn get_knowledge_collection_merkle_root(
        &self,
        blockchain: &BlockchainId,
        knowledge_collection_id: u128,
    ) -> Result<Option<String>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl
            .get_knowledge_collection_merkle_root(knowledge_collection_id)
            .await
    }

    // ==================== Paranet Methods ====================

    /// Check if a paranet exists on-chain.
    pub async fn paranet_exists(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
    ) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl.paranet_exists(paranet_id).await
    }

    /// Get the nodes access policy for a paranet.
    pub async fn get_nodes_access_policy(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
    ) -> Result<AccessPolicy, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl.get_nodes_access_policy(paranet_id).await
    }

    /// Get the list of permissioned nodes for a paranet.
    pub async fn get_permissioned_nodes(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
    ) -> Result<Vec<PermissionedNode>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl.get_permissioned_nodes(paranet_id).await
    }

    /// Check if a knowledge collection is registered in a paranet.
    pub async fn is_knowledge_collection_registered(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
        knowledge_collection_id: B256,
    ) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl
            .is_knowledge_collection_registered(paranet_id, knowledge_collection_id)
            .await
    }
}

#[cfg(test)]
mod tests {
    use blockchains::evm_chain::format_balance;

    use super::*;

    fn config_for_chain(chain_type: &str, chain_id: u64) -> BlockchainConfig {
        BlockchainConfig {
            blockchain_id: BlockchainId::new(format!("{}:{}", chain_type, chain_id)),
            evm_operational_wallet_private_key: String::new(),
            evm_operational_wallet_address: String::new(),
            evm_management_wallet_address: String::new(),
            evm_management_wallet_private_key: None,
            hub_contract_address: String::new(),
            rpc_endpoints: vec![],
            node_name: String::new(),
            gas_price_oracle_url: None,
            operator_fee: None,
            initial_stake_amount: None,
            initial_ask_amount: None,
            substrate_rpc_endpoints: None,
        }
    }

    #[test]
    fn test_blockchain_id_parsing() {
        let id = BlockchainId::new("otp:2043");
        assert_eq!(id.prefix(), "otp");
        assert_eq!(id.chain_id(), Some(2043));

        let id = BlockchainId::new("hardhat1:31337");
        assert_eq!(id.prefix(), "hardhat1");
        assert_eq!(id.chain_id(), Some(31337));

        // Missing chain_id
        let id = BlockchainId::new("hardhat1");
        assert_eq!(id.prefix(), "hardhat1");
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
    fn test_blockchain_is_development_chain() {
        assert!(Blockchain::Hardhat(config_for_chain("hardhat1", 31337)).is_development_chain());
        assert!(Blockchain::Hardhat(config_for_chain("hardhat2", 31337)).is_development_chain());
        assert!(!Blockchain::Gnosis(config_for_chain("gnosis", 100)).is_development_chain());
        assert!(!Blockchain::NeuroWeb(config_for_chain("otp", 2043)).is_development_chain());
        assert!(!Blockchain::Base(config_for_chain("base", 8453)).is_development_chain());
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
