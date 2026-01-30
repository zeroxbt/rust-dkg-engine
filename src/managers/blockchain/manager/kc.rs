use crate::managers::blockchain::*;

impl BlockchainManager {
    /// Check if a knowledge collection exists on-chain.
    ///
    /// Returns the publisher address if the collection exists, or None if it doesn't.
    /// This is used to validate UALs before sending get requests.
    pub(crate) async fn get_knowledge_collection_publisher(
        &self,
        blockchain: &BlockchainId,
        contract_address: Address,
        knowledge_collection_id: u128,
    ) -> Result<Option<Address>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_knowledge_collection_publisher(contract_address, knowledge_collection_id)
            .await
    }

    /// Get the range of knowledge assets (token IDs) for a knowledge collection.
    ///
    /// Returns (start_token_id, end_token_id, burned_token_ids) or None if the collection
    /// doesn't exist.
    pub(crate) async fn get_knowledge_assets_range(
        &self,
        blockchain: &BlockchainId,
        contract_address: Address,
        knowledge_collection_id: u128,
    ) -> Result<Option<(u64, u64, Vec<u64>)>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_knowledge_assets_range(contract_address, knowledge_collection_id)
            .await
    }

    /// Get the latest merkle root for a knowledge collection.
    ///
    /// Returns the merkle root as a hex string (with 0x prefix), or None if the collection
    /// doesn't exist.
    pub(crate) async fn get_knowledge_collection_merkle_root(
        &self,
        blockchain: &BlockchainId,
        contract_address: Address,
        knowledge_collection_id: u128,
    ) -> Result<Option<String>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_knowledge_collection_merkle_root(contract_address, knowledge_collection_id)
            .await
    }

    /// Execute a batch of heterogeneous calls using Multicall3.
    ///
    /// This allows combining different types of calls (e.g., getTokenRange + getEndEpoch)
    /// into a single RPC request, reducing round-trips.
    pub(crate) async fn execute_multicall(
        &self,
        blockchain: &BlockchainId,
        batch: multicall::MulticallBatch,
    ) -> Result<Vec<multicall::MulticallResult>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.execute_multicall(batch).await
    }

    /// Get the latest knowledge collection ID for a contract.
    ///
    /// Returns the highest KC ID that has been created on this contract.
    /// Returns 0 if no collections have been created yet.
    pub(crate) async fn get_latest_knowledge_collection_id(
        &self,
        blockchain: &BlockchainId,
        contract_address: Address,
    ) -> Result<u64, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_latest_knowledge_collection_id(contract_address)
            .await
    }

    /// Get the current epoch from the Chronos contract.
    pub(crate) async fn get_current_epoch(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<u64, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_current_epoch().await
    }
}
