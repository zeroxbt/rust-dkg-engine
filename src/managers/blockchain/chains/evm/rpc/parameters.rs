use crate::managers::blockchain::{chains::evm::EvmChain, error::BlockchainError};

impl EvmChain {
    pub(crate) async fn get_minimum_required_signatures(&self) -> Result<u64, BlockchainError> {
        use alloy::primitives::U256;
        let contracts = self.contracts().await;
        let min_signatures: U256 = self
            .rpc_call(
                contracts
                    .parameters_storage()
                    .minimumRequiredSignatures()
                    .call(),
            )
            .await?;
        Ok(min_signatures.to::<u64>())
    }
}
