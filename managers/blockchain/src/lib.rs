mod blockchains;
pub mod utils;

use blockchains::{
    abstract_blockchain::{AbstractBlockchain, BlockchainError},
    blockchain_creator::BlockchainCreator,
};
pub use blockchains::{
    abstract_blockchain::{ContractName, EventLog, EventName},
    blockchain_creator::{
        AskUpdatedFilter, AssetStorageChangedFilter, ContractChangedFilter, NewAssetStorageFilter,
        NewContractFilter, NodeAddedFilter, NodeRemovedFilter, StakeIncreasedFilter,
        StakeWithdrawalStartedFilter,
    },
};
use ethers::types::Address;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize)]
pub struct BlockchainConfig {
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum BlockchainName {
    Hardhat,
}

impl BlockchainName {
    pub fn as_str(&self) -> &str {
        match self {
            BlockchainName::Hardhat => "Hardhat",
        }
    }
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
pub struct BlockchainManagerConfig(pub Vec<Blockchain>);

pub struct BlockchainManager {
    blockchains: HashMap<BlockchainName, Box<dyn AbstractBlockchain>>,
}

impl BlockchainManager {
    pub async fn new(config: &BlockchainManagerConfig) -> Self {
        let mut blockchains = HashMap::new();
        for blockchain in config.0.iter() {
            match blockchain {
                Blockchain::Hardhat(blockchain_config) => {
                    let blockchain =
                        blockchains::hardhat::HardhatBlockchain::new(blockchain_config.clone())
                            .await;
                    let blockchain_name = blockchain.name().to_owned();
                    let trait_object: Box<dyn AbstractBlockchain> = Box::new(blockchain);

                    blockchains.insert(blockchain_name, trait_object);
                }
            }
        }

        Self { blockchains }
    }

    pub fn get_blockchain_names(&self) -> Vec<&BlockchainName> {
        self.blockchains.keys().collect()
    }

    pub fn get_blockchain_config(&self, blockchain: &BlockchainName) -> &BlockchainConfig {
        self.blockchains.get(blockchain).unwrap().config()
    }

    pub async fn identity_id_exists(&self, blockchain: &BlockchainName) -> bool {
        self.blockchains
            .get(blockchain)
            .unwrap()
            .identity_id_exists()
            .await
    }

    pub async fn get_identity_id(&self, blockchain: &BlockchainName) -> Option<u128> {
        self.blockchains
            .get(blockchain)
            .unwrap()
            .get_identity_id()
            .await
    }

    pub async fn initialize_identities(&mut self, peer_id: &str) -> Result<(), BlockchainError> {
        for blockchain in self.blockchains.values_mut() {
            blockchain.initialize_identity(peer_id).await?;
        }

        Ok(())
    }

    pub async fn get_event_logs(
        &self,
        blockchain: &BlockchainName,
        contract_name: &ContractName,
        events_to_filter: &Vec<EventName>,
        from_block: u64,
        current_block: u64,
    ) -> Result<Vec<EventLog>, BlockchainError> {
        self.blockchains
            .get(blockchain)
            .unwrap()
            .get_event_logs(contract_name, events_to_filter, from_block, current_block)
            .await
    }

    pub async fn get_block_number(
        &self,
        blockchain: &BlockchainName,
    ) -> Result<u64, BlockchainError> {
        self.blockchains
            .get(blockchain)
            .unwrap()
            .get_block_number()
            .await
    }

    pub async fn re_initialize_contract(
        &self,
        blockchain: &BlockchainName,
        contract_name: String,
        contract_address: Address,
    ) -> Result<(), BlockchainError> {
        self.blockchains
            .get(blockchain)
            .unwrap()
            .re_initialize_contract(contract_name, contract_address)
            .await
    }
}
