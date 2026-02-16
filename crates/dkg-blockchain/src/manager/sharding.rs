use dkg_domain::BlockchainId;

use crate::{
    BlockchainManager,
    {chains::evm::NodeInfo, error::BlockchainError},
};

impl BlockchainManager {
    pub async fn get_sharding_table_head(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u128, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_sharding_table_head().await
    }

    pub async fn get_minimum_required_signatures(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u64, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_minimum_required_signatures().await
    }

    pub async fn get_sharding_table_length(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u128, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_sharding_table_length().await
    }

    pub async fn get_sharding_table_page(
        &self,
        blockchain: &BlockchainId,
        starting_identity_id: u128,
        nodes_num: u128,
    ) -> Result<Vec<NodeInfo>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_sharding_table_page(starting_identity_id, nodes_num)
            .await
    }

    pub async fn sharding_table_node_exists(
        &self,
        blockchain: &BlockchainId,
        identity_id: u128,
    ) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .sharding_table_node_exists(identity_id)
            .await
    }

    pub async fn get_sharding_table_node(
        &self,
        blockchain: &BlockchainId,
        identity_id: u128,
    ) -> Result<Option<crate::chains::evm::ShardingTableNode>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_sharding_table_node(identity_id).await
    }
}
