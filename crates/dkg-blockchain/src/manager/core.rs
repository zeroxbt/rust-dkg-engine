use std::collections::HashMap;

use dkg_domain::BlockchainId;

use super::super::chains::evm::EvmChain;
use crate::{BlockchainManager, BlockchainManagerConfig, error::BlockchainError};

impl BlockchainManager {
    pub async fn connect(config: &BlockchainManagerConfig) -> Result<Self, BlockchainError> {
        let mut blockchains = HashMap::new();

        for blockchain in &config.0 {
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
            .await
            .map_err(|source| BlockchainError::BlockchainInitialization {
                blockchain_id: blockchain_id.to_string(),
                source: Box::new(source),
            })?;
            blockchains.insert(blockchain_id, evm_chain);
        }

        Ok(Self { blockchains })
    }

    pub fn get_blockchain_ids(&self) -> Vec<&BlockchainId> {
        self.blockchains.keys().collect()
    }

    pub fn chain(&self, blockchain: &BlockchainId) -> Result<&EvmChain, BlockchainError> {
        self.blockchains
            .get(blockchain)
            .ok_or_else(|| BlockchainError::BlockchainNotFound {
                blockchain_id: blockchain.as_str().to_string(),
            })
    }
}
