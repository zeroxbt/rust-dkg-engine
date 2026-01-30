use crate::managers::blockchain::*;

impl BlockchainManager {
    /// Get the current gas price for a blockchain.
    pub(crate) async fn get_gas_price(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<U256, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        Ok(blockchain_impl.get_gas_price().await)
    }

    /// Get the gas configuration for a blockchain.
    pub(crate) fn get_gas_config(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<&GasConfig, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        Ok(blockchain_impl.gas_config())
    }

    /// Get the native token decimals for a blockchain.
    pub(crate) fn get_native_token_decimals(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u8, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        Ok(blockchain_impl.native_token_decimals())
    }

    /// Get the native token ticker for a blockchain.
    pub(crate) fn get_native_token_ticker(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<&'static str, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        Ok(blockchain_impl.native_token_ticker())
    }

    /// Check if a blockchain is a development chain.
    pub(crate) fn is_development_chain(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        Ok(blockchain_impl.is_development_chain())
    }

    /// Get the native token balance for an address, formatted with correct decimals.
    pub(crate) async fn get_native_token_balance(
        &self,
        blockchain: &BlockchainId,
        address: Address,
    ) -> Result<String, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_native_token_balance(address).await
    }
}
