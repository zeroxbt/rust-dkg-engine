use alloy::primitives::Address;
use dkg_domain::BlockchainId;

use crate::managers::{
    BlockchainManager,
    blockchain::{ContractName, error::BlockchainError},
};

impl BlockchainManager {
    pub(crate) async fn re_initialize_contract(
        &self,
        blockchain: &BlockchainId,
        contract_name: String,
        contract_address: Address,
    ) -> Result<(), BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .re_initialize_contract(contract_name, contract_address)
            .await
    }

    /// Get all contract addresses for a contract type on a blockchain.
    pub(crate) async fn get_all_contract_addresses(
        &self,
        blockchain: &BlockchainId,
        contract_name: &ContractName,
    ) -> Result<Vec<Address>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        Ok(blockchain_impl
            .get_all_contract_addresses(contract_name)
            .await)
    }
}
