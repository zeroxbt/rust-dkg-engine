use super::super::*;

impl EvmChain {
    pub(crate) async fn get_sharding_table_head(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let contracts = self.contracts().await;
        let head: Uint<72, 2> = self
            .rpc_call(contracts.sharding_table_storage().head().call())
            .await?;
        Ok(head.to::<u128>())
    }

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

    pub(crate) async fn get_sharding_table_length(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let contracts = self.contracts().await;
        let nodes_count: Uint<72, 2> = self
            .rpc_call(contracts.sharding_table_storage().nodesCount().call())
            .await?;
        Ok(nodes_count.to::<u128>())
    }

    pub(crate) async fn get_sharding_table_page(
        &self,
        starting_identity_id: u128,
        nodes_num: u128,
    ) -> Result<Vec<NodeInfo>, BlockchainError> {
        use alloy::primitives::Uint;
        let contracts = self.contracts().await;
        let nodes = self
            .rpc_call(
                contracts
                    .sharding_table()
                    .getShardingTable_1(
                        Uint::<72, 2>::from(starting_identity_id),
                        Uint::<72, 2>::from(nodes_num),
                    )
                    .call(),
            )
            .await?;

        Ok(nodes)
    }
}
