use crate::managers::blockchain::{chains::evm::EvmChain, error::BlockchainError};

impl EvmChain {
    pub(crate) async fn get_minimum_required_signatures(&self) -> Result<u64, BlockchainError> {
        use alloy::primitives::U256;
        let min_signatures: U256 = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts
                    .parameters_storage()
                    .minimumRequiredSignatures()
                    .call()
                    .await
            })
            .await?;
        Ok(min_signatures.to::<u64>())
    }
}
