use crate::blockchain_creator::{BlockchainCreator, BlockchainProvider, Contracts};
use crate::blockchains::abstract_blockchain::AbstractBlockchain;
use crate::BlockchainConfig;

use std::sync::Arc;

use async_trait::async_trait;

pub struct HardhatBlockchain {
    config: BlockchainConfig,
    provider: Arc<BlockchainProvider>,
    contracts: Contracts,
}

#[async_trait]
impl AbstractBlockchain for HardhatBlockchain {
    fn get_name(&self) -> String {
        "hardhat".to_string()
    }
    fn get_config(&self) -> &BlockchainConfig {
        &self.config
    }
    fn get_provider(&self) -> Arc<BlockchainProvider> {
        Arc::clone(&self.provider)
    }

    fn get_contracts(&self) -> &Contracts {
        &self.contracts
    }
}

#[async_trait]
impl BlockchainCreator for HardhatBlockchain {
    async fn new(config: BlockchainConfig) -> Self {
        let provider = Self::create_ethers_provider(&config).await.unwrap();
        let contracts = Self::initialize_contracts(&config, Arc::clone(&provider)).await;

        Self {
            provider,
            config,
            contracts,
        }
    }
}
