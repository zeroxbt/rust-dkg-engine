use crate::managers::blockchain::{
    chains::evm::{EvmChain, NodeInfo},
    error::BlockchainError,
};

impl EvmChain {
    pub(crate) async fn get_sharding_table_head(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let head: Uint<72, 2> = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts.sharding_table_storage().head().call().await
            })
            .await?;
        Ok(head.to::<u128>())
    }

    pub(crate) async fn get_sharding_table_length(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let nodes_count: Uint<72, 2> = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts.sharding_table_storage().nodesCount().call().await
            })
            .await?;
        Ok(nodes_count.to::<u128>())
    }

    pub(crate) async fn get_sharding_table_page(
        &self,
        starting_identity_id: u128,
        nodes_num: u128,
    ) -> Result<Vec<NodeInfo>, BlockchainError> {
        use alloy::primitives::Uint;
        let nodes = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts
                    .sharding_table()
                    .getShardingTable_1(
                        Uint::<72, 2>::from(starting_identity_id),
                        Uint::<72, 2>::from(nodes_num),
                    )
                    .call()
                    .await
            })
            .await?;

        Ok(nodes)
    }
}
