pub mod blockchains;
pub mod error;
pub mod utils;
use std::collections::HashMap;

use blockchains::{abstract_blockchain::AbstractBlockchain, blockchain_creator::BlockchainCreator};
pub use blockchains::{
    abstract_blockchain::{ContractName, EventLog, EventName},
    blockchain_creator::{
        AssetStorageChangedFilter, ContractChangedFilter, KnowledgeCollectionCreatedFilter,
        NewAssetStorageFilter, NewContractFilter, ParameterChangedFilter, ShardingTableNode,
    },
};
pub use ethers::{
    abi::Token,
    types::{Address, H256, U256},
};
use serde::{Deserialize, Serialize};

use crate::error::BlockchainError;

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
}

#[derive(Debug, Deserialize, Clone)]
pub enum Blockchain {
    Hardhat(BlockchainConfig),
}

impl Blockchain {
    pub fn get_config(&self) -> &BlockchainConfig {
        match self {
            Blockchain::Hardhat(config) => config,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct BlockchainManagerConfig(pub Vec<Blockchain>);

pub struct BlockchainManager {
    blockchains: HashMap<BlockchainId, Box<dyn AbstractBlockchain>>,
}

impl BlockchainManager {
    pub async fn new(config: &BlockchainManagerConfig) -> Self {
        let mut blockchains = HashMap::new();
        for blockchain in config.0.iter() {
            match blockchain {
                Blockchain::Hardhat(blockchain_config) => {
                    let mut config = blockchain_config.clone();
                    let blockchain_id = config.blockchain_id.clone().unwrap_or_else(|| {
                        BlockchainId::new(format!("hardhat:{}", config.chain_id()))
                    });
                    config.blockchain_id = Some(blockchain_id.clone());
                    let blockchain = blockchains::hardhat::HardhatBlockchain::new(config).await;
                    let trait_object: Box<dyn AbstractBlockchain> = Box::new(blockchain);

                    blockchains.insert(blockchain_id, trait_object);
                }
            }
        }

        Self { blockchains }
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
                blockchain: blockchain.as_str().to_string(),
            })
    }

    pub async fn identity_id_exists(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain: blockchain.as_str().to_string(),
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
                blockchain: blockchain.as_str().to_string(),
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
        events_to_filter: &Vec<EventName>,
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<EventLog>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl
            .get_event_logs(contract_name, events_to_filter, from_block, current_block)
            .await
    }

    pub async fn get_block_number(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u64, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain: blockchain.as_str().to_string(),
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
                blockchain: blockchain.as_str().to_string(),
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
                blockchain: blockchain.as_str().to_string(),
            }
        })?;

        blockchain_impl.get_sharding_table_length().await
    }

    pub async fn get_sharding_table_page(
        &self,
        blockchain: &BlockchainId,
        starting_identity_id: u128,
        nodes_num: u128,
    ) -> Result<Vec<ShardingTableNode>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain: blockchain.as_str().to_string(),
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
                blockchain: blockchain.as_str().to_string(),
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
    ) -> Result<Vec<u8>, BlockchainError> {
        let blockchain_impl = self.blockchains.get(blockchain).ok_or_else(|| {
            BlockchainError::BlockchainNotFound {
                blockchain: blockchain.as_str().to_string(),
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
                blockchain: blockchain.as_str().to_string(),
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
                blockchain: blockchain.as_str().to_string(),
            }
        })?;
        blockchain_impl.set_ask(ask_wei).await
    }
}
