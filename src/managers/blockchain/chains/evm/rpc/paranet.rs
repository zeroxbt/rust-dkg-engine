use alloy::{contract::Error as ContractError, primitives::B256, transports::TransportErrorKind};

use crate::{
    managers::blockchain::{
        chains::evm::{EvmChain, contracts::PermissionedNode},
        error::BlockchainError,
    },
    types::AccessPolicy,
};

impl EvmChain {
    /// Check if a paranet exists on-chain.
    pub(crate) async fn paranet_exists(&self, paranet_id: B256) -> Result<bool, BlockchainError> {
        let exists = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                let registry = contracts.paranets_registry().map_err(|err| {
                    ContractError::TransportError(TransportErrorKind::custom_str(&err.to_string()))
                })?;
                registry.paranetExists(paranet_id).call().await
            })
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
        let policy = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                let registry = contracts.paranets_registry().map_err(|err| {
                    ContractError::TransportError(TransportErrorKind::custom_str(&err.to_string()))
                })?;
                registry.getNodesAccessPolicy(paranet_id).call().await
            })
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
        let nodes = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                let registry = contracts.paranets_registry().map_err(|err| {
                    ContractError::TransportError(TransportErrorKind::custom_str(&err.to_string()))
                })?;
                registry.getPermissionedNodes(paranet_id).call().await
            })
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!("Failed to get permissioned nodes: {}", e))
            })?;

        Ok(nodes)
    }

    /// Check if a knowledge collection is registered in a paranet.
    pub(crate) async fn is_knowledge_collection_registered(
        &self,
        paranet_id: B256,
        knowledge_collection_id: B256,
    ) -> Result<bool, BlockchainError> {
        let registered = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                let registry = contracts.paranets_registry().map_err(|err| {
                    ContractError::TransportError(TransportErrorKind::custom_str(&err.to_string()))
                })?;
                registry
                    .isKnowledgeCollectionRegistered(paranet_id, knowledge_collection_id)
                    .call()
                    .await
            })
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
