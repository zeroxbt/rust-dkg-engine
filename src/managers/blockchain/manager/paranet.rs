use crate::{
    managers::blockchain::*,
    types::{AccessPolicy, PermissionedNode},
};

impl BlockchainManager {
    /// Check if a paranet exists on-chain.
    pub(crate) async fn paranet_exists(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
    ) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.paranet_exists(paranet_id).await
    }

    /// Get the nodes access policy for a paranet.
    pub(crate) async fn get_nodes_access_policy(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
    ) -> Result<AccessPolicy, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_nodes_access_policy(paranet_id).await
    }

    /// Get the list of permissioned nodes for a paranet.
    pub(crate) async fn get_permissioned_nodes(
        &self,
        blockchain: &BlockchainId,
        paranet_id: B256,
    ) -> Result<Vec<PermissionedNode>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_permissioned_nodes(paranet_id).await
    }

    /// Check if a knowledge collection is registered in a paranet.
    pub(crate) async fn is_knowledge_collection_registered(
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
}
