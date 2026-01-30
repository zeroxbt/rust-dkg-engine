use super::super::*;

impl EvmChain {
    /// Check if a paranet exists on-chain.
    pub(crate) async fn paranet_exists(&self, paranet_id: B256) -> Result<bool, BlockchainError> {
        let contracts = self.contracts().await;
        let registry = contracts.paranets_registry()?;

        let exists = self
            .rpc_call(registry.paranetExists(paranet_id).call())
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!("Failed to check paranet exists: {}", e))
            })?;

        Ok(exists)
    }

    /// Get the nodes access policy for a paranet.
    pub(crate) async fn get_nodes_access_policy(
        &self,
        paranet_id: B256,
    ) -> Result<AccessPolicy, BlockchainError> {
        let contracts = self.contracts().await;
        let registry = contracts.paranets_registry()?;

        let policy = self
            .rpc_call(registry.getNodesAccessPolicy(paranet_id).call())
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!("Failed to get nodes access policy: {}", e))
            })?;

        Ok(AccessPolicy::from(policy))
    }

    /// Get the list of permissioned nodes for a paranet.
    pub(crate) async fn get_permissioned_nodes(
        &self,
        paranet_id: B256,
    ) -> Result<Vec<PermissionedNode>, BlockchainError> {
        let contracts = self.contracts().await;
        let registry = contracts.paranets_registry()?;

        let nodes = self
            .rpc_call(registry.getPermissionedNodes(paranet_id).call())
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!("Failed to get permissioned nodes: {}", e))
            })?;

        Ok(nodes
            .into_iter()
            .map(|node| PermissionedNode {
                identity_id: node.identityId.to::<u128>(),
                node_id: node.nodeId.to_vec(),
            })
            .collect())
    }

    /// Check if a knowledge collection is registered in a paranet.
    pub(crate) async fn is_knowledge_collection_registered(
        &self,
        paranet_id: B256,
        knowledge_collection_id: B256,
    ) -> Result<bool, BlockchainError> {
        let contracts = self.contracts().await;
        let registry = contracts.paranets_registry()?;

        let registered = self
            .rpc_call(
                registry
                    .isKnowledgeCollectionRegistered(paranet_id, knowledge_collection_id)
                    .call(),
            )
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to check knowledge collection registration: {}",
                    e
                ))
            })?;

        Ok(registered)
    }
}
