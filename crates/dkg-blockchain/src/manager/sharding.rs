use dkg_domain::BlockchainId;

use crate::{BlockchainManager, NodeInfo, ShardingTableNode, error::BlockchainError};

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
        if let Some(cached) = self
            .parameter_cache
            .get_minimum_required_signatures(blockchain)
            .await
        {
            return Ok(cached);
        }

        let blockchain_impl = self.chain(blockchain)?;
        let value = blockchain_impl.get_minimum_required_signatures().await?;
        self.parameter_cache
            .set_minimum_required_signatures(blockchain, value)
            .await;
        Ok(value)
    }

    pub async fn set_cached_minimum_required_signatures(
        &self,
        blockchain: &BlockchainId,
        value: u64,
    ) {
        self.parameter_cache
            .set_minimum_required_signatures(blockchain, value)
            .await;
    }

    pub async fn invalidate_cached_minimum_required_signatures(&self, blockchain: &BlockchainId) {
        self.parameter_cache
            .invalidate_minimum_required_signatures(blockchain)
            .await;
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
    ) -> Result<Option<ShardingTableNode>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_sharding_table_node(identity_id).await
    }
}
