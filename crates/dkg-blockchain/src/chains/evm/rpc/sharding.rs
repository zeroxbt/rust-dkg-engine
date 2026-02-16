use crate::{
    chains::evm::{EvmChain, NodeInfo, ShardingTableNode},
    error::BlockchainError,
};

impl EvmChain {
    pub async fn get_sharding_table_head(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let head: Uint<72, 2> = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts.sharding_table_storage().head().call().await
            })
            .await?;
        Ok(head.to::<u128>())
    }

    pub async fn get_sharding_table_length(&self) -> Result<u128, BlockchainError> {
        use alloy::primitives::Uint;
        let nodes_count: Uint<72, 2> = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts.sharding_table_storage().nodesCount().call().await
            })
            .await?;
        Ok(nodes_count.to::<u128>())
    }

    pub async fn get_sharding_table_page(
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

    pub async fn sharding_table_node_exists(
        &self,
        identity_id: u128,
    ) -> Result<bool, BlockchainError> {
        use alloy::primitives::Uint;
        let exists: bool = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts
                    .sharding_table_storage()
                    .nodeExists(Uint::<72, 2>::from(identity_id))
                    .call()
                    .await
            })
            .await?;
        Ok(exists)
    }

    pub async fn get_sharding_table_node(
        &self,
        identity_id: u128,
    ) -> Result<Option<ShardingTableNode>, BlockchainError> {
        use alloy::primitives::Uint;

        if !self.sharding_table_node_exists(identity_id).await? {
            return Ok(None);
        }

        let node: ShardingTableNode = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts
                    .sharding_table_storage()
                    .getNode(Uint::<72, 2>::from(identity_id))
                    .call()
                    .await
            })
            .await?;

        Ok(Some(node))
    }
}
