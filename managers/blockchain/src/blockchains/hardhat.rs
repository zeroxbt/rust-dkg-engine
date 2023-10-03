use crate::blockchains::{
    abstract_blockchain::AbstractBlockchain,
    blockchain_creator::{BlockchainCreator, BlockchainProvider, Contracts},
};
use crate::{BlockchainConfig, BlockchainName};

use std::sync::Arc;

use async_trait::async_trait;

pub struct HardhatBlockchain {
    config: BlockchainConfig,
    provider: Arc<BlockchainProvider>,
    contracts: Contracts,
    identity_id: Option<u128>,
}

#[async_trait]
impl AbstractBlockchain for HardhatBlockchain {
    fn name(&self) -> &BlockchainName {
        &BlockchainName::Hardhat
    }
    fn config(&self) -> &BlockchainConfig {
        &self.config
    }
    fn provider(&self) -> &Arc<BlockchainProvider> {
        &self.provider
    }

    fn contracts(&self) -> &Contracts {
        &self.contracts
    }

    fn set_identity_id(&mut self, id: u128) {
        self.identity_id = Some(id);
    }
}

#[async_trait]
impl BlockchainCreator for HardhatBlockchain {
    async fn new(config: BlockchainConfig) -> Self {
        let provider = Self::initialize_ethers_provider(&config).await.unwrap();
        let contracts = Self::initialize_contracts(&config, &provider).await;

        Self {
            provider,
            config,
            contracts,
            identity_id: None,
        }
    }
}
