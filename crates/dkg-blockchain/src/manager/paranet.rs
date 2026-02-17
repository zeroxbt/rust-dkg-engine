use alloy::primitives::B256;
use dkg_domain::{AccessPolicy, BlockchainId, ParanetKcLocator};

use crate::{BlockchainManager, chains::evm::PermissionedNode, error::BlockchainError};

impl BlockchainManager {
    /// Check if a paranet exists on-chain.
    pub async fn paranet_exists(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
    ) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.paranet_exists(paranet_id).await
    }

    /// Get the nodes access policy for a paranet.
    pub async fn get_nodes_access_policy(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
    ) -> Result<AccessPolicy, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_nodes_access_policy(paranet_id).await
    }

    /// Get the list of permissioned nodes for a paranet.
    pub async fn get_permissioned_nodes(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
    ) -> Result<Vec<PermissionedNode>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_permissioned_nodes(paranet_id).await
    }

    /// Check if a knowledge collection is registered in a paranet.
    pub async fn is_knowledge_collection_registered(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
        knowledge_collection_id: B256,
    ) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .is_knowledge_collection_registered(paranet_id, knowledge_collection_id)
            .await
    }

    /// Get the total number of knowledge collections registered in a paranet.
    pub async fn get_paranet_knowledge_collection_count(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
    ) -> Result<u64, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_paranet_knowledge_collection_count(paranet_id)
            .await
    }

    /// Get knowledge collection locators from a paranet with pagination.
    pub async fn get_paranet_knowledge_collection_locators_with_pagination(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<ParanetKcLocator>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_paranet_knowledge_collection_locators_with_pagination(paranet_id, offset, limit)
            .await
    }
}
