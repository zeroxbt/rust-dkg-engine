use super::super::*;

impl EvmChain {
    /// Check if a knowledge collection exists on-chain.
    ///
    /// Validates that the knowledge collection ID exists by checking if it has a publisher.
    /// Returns the publisher address if the collection exists, or None if it doesn't.
    ///
    /// Returns an error if the contract address is not a registered KnowledgeCollectionStorage.
    /// The node only works with contracts discovered at initialization or via NewAssetStorage
    /// events.
    pub(crate) async fn get_knowledge_collection_publisher(
        &self,
        contract_address: Address,
        knowledge_collection_id: u128,
    ) -> Result<Option<Address>, BlockchainError> {
        let contracts = self.contracts().await;
        let kc_storage = contracts
                .knowledge_collection_storage_by_address(&contract_address)
                .ok_or_else(|| {
                    BlockchainError::Custom(format!(
                        "KnowledgeCollectionStorage at {:?} is not registered. \
                         The node only knows about contracts from initialization or NewAssetStorage events.",
                        contract_address
                    ))
                })?;

        let publisher = self
            .rpc_call(
                kc_storage
                    .getLatestMerkleRootPublisher(U256::from(knowledge_collection_id))
                    .call(),
            )
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get knowledge collection publisher from {:?}: {}",
                    contract_address, e
                ))
            })?;

        // If publisher is zero address, the collection doesn't exist
        if publisher.is_zero() {
            Ok(None)
        } else {
            Ok(Some(publisher))
        }
    }

    /// Get the range of knowledge assets (token IDs) for a knowledge collection.
    ///
    /// Returns (start_token_id, end_token_id, burned_token_ids) or None if the collection
    /// doesn't exist.
    ///
    /// Returns an error if the contract address is not a registered KnowledgeCollectionStorage.
    pub(crate) async fn get_knowledge_assets_range(
        &self,
        contract_address: Address,
        knowledge_collection_id: u128,
    ) -> Result<Option<(u64, u64, Vec<u64>)>, BlockchainError> {
        let contracts = self.contracts().await;
        let kc_storage = contracts
                .knowledge_collection_storage_by_address(&contract_address)
                .ok_or_else(|| {
                    BlockchainError::Custom(format!(
                        "KnowledgeCollectionStorage at {:?} is not registered. \
                         The node only knows about contracts from initialization or NewAssetStorage events.",
                        contract_address
                    ))
                })?;

        let result = self
            .rpc_call(
                kc_storage
                    .getKnowledgeAssetsRange(U256::from(knowledge_collection_id))
                    .call(),
            )
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get knowledge assets range from {:?}: {}",
                    contract_address, e
                ))
            })?;

        let start_token_id = result
            ._0
            .try_into()
            .map_err(|_| BlockchainError::Custom("Start token ID overflow".to_string()))?;
        let end_token_id = result
            ._1
            .try_into()
            .map_err(|_| BlockchainError::Custom("End token ID overflow".to_string()))?;
        let burned: Vec<u64> = result
            ._2
            .into_iter()
            .filter_map(|v| v.try_into().ok())
            .collect();

        // If both start and end are 0, the collection doesn't exist or is empty
        if start_token_id == 0 && end_token_id == 0 {
            Ok(None)
        } else {
            Ok(Some((start_token_id, end_token_id, burned)))
        }
    }

    /// Get the latest merkle root for a knowledge collection.
    ///
    /// Returns the merkle root as a hex string (with 0x prefix), or None if the collection
    /// doesn't exist.
    ///
    /// Returns an error if the contract address is not a registered KnowledgeCollectionStorage.
    pub(crate) async fn get_knowledge_collection_merkle_root(
        &self,
        contract_address: Address,
        knowledge_collection_id: u128,
    ) -> Result<Option<String>, BlockchainError> {
        let contracts = self.contracts().await;
        let kc_storage = contracts
                .knowledge_collection_storage_by_address(&contract_address)
                .ok_or_else(|| {
                    BlockchainError::Custom(format!(
                        "KnowledgeCollectionStorage at {:?} is not registered. \
                         The node only knows about contracts from initialization or NewAssetStorage events.",
                        contract_address
                    ))
                })?;

        let merkle_root = self
            .rpc_call(
                kc_storage
                    .getLatestMerkleRoot(U256::from(knowledge_collection_id))
                    .call(),
            )
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get knowledge collection merkle root from {:?}: {}",
                    contract_address, e
                ))
            })?;

        // If merkle root is zero, the collection doesn't exist
        if merkle_root.is_zero() {
            Ok(None)
        } else {
            Ok(Some(format!(
                "0x{}",
                crate::managers::blockchain::utils::to_hex_string(merkle_root)
            )))
        }
    }

    /// Get the latest knowledge collection ID for a contract.
    ///
    /// Returns the highest KC ID that has been created on this contract.
    /// Returns 0 if no collections have been created yet.
    pub(crate) async fn get_latest_knowledge_collection_id(
        &self,
        contract_address: Address,
    ) -> Result<u64, BlockchainError> {
        let contracts = self.contracts().await;
        let kc_storage = contracts
                .knowledge_collection_storage_by_address(&contract_address)
                .ok_or_else(|| {
                    BlockchainError::Custom(format!(
                        "KnowledgeCollectionStorage at {:?} is not registered. \
                         The node only knows about contracts from initialization or NewAssetStorage events.",
                        contract_address
                    ))
                })?;

        let latest_id = self
            .rpc_call(kc_storage.getLatestKnowledgeCollectionId().call())
            .await
            .map_err(|e| {
                BlockchainError::Custom(format!(
                    "Failed to get latest knowledge collection ID from {:?}: {}",
                    contract_address, e
                ))
            })?;

        latest_id.try_into().map_err(|_| {
            BlockchainError::Custom("Latest knowledge collection ID overflow".to_string())
        })
    }

    /// Get the current epoch from the Chronos contract.
    pub(crate) async fn get_current_epoch(&self) -> Result<u64, BlockchainError> {
        let contracts = self.contracts().await;
        let chronos = contracts.chronos();

        let current_epoch = self
            .rpc_call(chronos.getCurrentEpoch().call())
            .await
            .map_err(|e| BlockchainError::Custom(format!("Failed to get current epoch: {}", e)))?;

        current_epoch
            .try_into()
            .map_err(|_| BlockchainError::Custom("Current epoch overflow".to_string()))
    }
}
