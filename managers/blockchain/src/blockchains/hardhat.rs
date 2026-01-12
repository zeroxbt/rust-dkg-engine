use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    BlockchainConfig, BlockchainName,
    blockchains::{
        abstract_blockchain::AbstractBlockchain,
        blockchain_creator::{BlockchainCreator, BlockchainProvider, Contracts},
    },
};

pub struct HardhatBlockchain {
    config: BlockchainConfig,
    provider: Arc<BlockchainProvider>,
    contracts: RwLock<Contracts>,
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

    async fn contracts(&self) -> RwLockReadGuard<'_, Contracts> {
        self.contracts.read().await
    }

    async fn contracts_mut(&self) -> RwLockWriteGuard<'_, Contracts> {
        self.contracts.write().await
    }

    fn set_identity_id(&mut self, id: u128) {
        self.identity_id = Some(id);
    }
}

impl BlockchainCreator for HardhatBlockchain {
    async fn new(config: BlockchainConfig) -> Self {
        let provider = Self::initialize_ethers_provider(&config)
            .await
            .expect("Failed to initialize blockchain provider");
        let contracts = Self::initialize_contracts(&config, &provider)
            .await
            .expect("Failed to initialize blockchain contracts");

        Self {
            provider,
            config,
            contracts: RwLock::new(contracts),
            identity_id: None,
        }
    }
}
