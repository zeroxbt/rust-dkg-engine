use alloy::{
    contract::Error as ContractError,
    primitives::{B256, U256},
    transports::TransportErrorKind,
};

use crate::{
    managers::blockchain::{
        chains::evm::{EvmChain, contracts::PermissionedNode},
        error::BlockchainError,
    },
    types::{AccessPolicy, ParanetKcLocator},
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

    /// Get the total number of knowledge collections registered in a paranet.
    pub(crate) async fn get_paranet_knowledge_collection_count(
        &self,
        paranet_id: B256,
    ) -> Result<u64, BlockchainError> {
        let count = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                let registry = contracts.paranets_registry().map_err(|err| {
                    ContractError::TransportError(TransportErrorKind::custom_str(&err.to_string()))
                })?;
                registry
                    .getKnowledgeCollectionsCount(paranet_id)
                    .call()
                    .await
            })
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get paranet knowledge collection count: {}",
                    e
                ))
            })?;

        count.try_into().map_err(|_| {
            BlockchainError::Custom("Paranet knowledge collection count overflow".to_string())
        })
    }

    /// Get knowledge collection locators from a paranet with pagination.
    pub(crate) async fn get_paranet_knowledge_collection_locators_with_pagination(
        &self,
        paranet_id: B256,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<ParanetKcLocator>, BlockchainError> {
        let locators = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                let paranet = contracts.paranet().map_err(|err| {
                    ContractError::TransportError(TransportErrorKind::custom_str(&err.to_string()))
                })?;
                paranet
                    .getKnowledgeCollectionLocatorsWithPagination(
                        paranet_id,
                        U256::from(offset),
                        U256::from(limit),
                    )
                    .call()
                    .await
            })
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get paranet knowledge collection locators: {}",
                    e
                ))
            })?;

        let mut result = Vec::with_capacity(locators.len());
        for locator in locators {
            let knowledge_collection_token_id: u128 =
                locator.knowledgeCollectionTokenId.try_into().map_err(|_| {
                    BlockchainError::Custom(
                        "Paranet knowledge collection token ID overflow".to_string(),
                    )
                })?;

            result.push(ParanetKcLocator {
                knowledge_collection_storage_contract: locator.knowledgeCollectionStorageContract,
                knowledge_collection_token_id,
            });
        }

        Ok(result)
    }
}
