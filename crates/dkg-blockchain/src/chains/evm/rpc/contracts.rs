use alloy::primitives::Address;

use crate::{ContractName, chains::evm::EvmChain, error::BlockchainError};

impl EvmChain {
    pub async fn re_initialize_contract(
        &self,
        contract_name: String,
        contract_address: Address,
    ) -> Result<(), BlockchainError> {
        let contract_name = contract_name
            .parse::<ContractName>()
            .map_err(BlockchainError::Custom)?;

        let provider = self.provider().await;
        let mut contracts = self.contracts_mut().await;

        contracts
            .replace_contract(&provider, contract_name, contract_address)
            .await
    }

    /// Get all contract addresses for a contract type.
    pub async fn get_all_contract_addresses(&self, contract_name: &ContractName) -> Vec<Address> {
        let contracts = self.contracts().await;
        contracts.get_all_addresses(contract_name)
    }
}
