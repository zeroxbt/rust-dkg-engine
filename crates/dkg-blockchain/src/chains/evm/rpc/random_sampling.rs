use alloy::primitives::{Address, FixedBytes, U256};

use crate::{
    chains::evm::{EvmChain, error_decode::decode_contract_error},
    error::BlockchainError,
};

/// Status of the current proof period.
#[derive(Debug, Clone)]
pub struct ProofPeriodStatus {
    pub active_proof_period_start_block: U256,
    pub is_valid: bool,
}

/// Challenge data from the RandomSamplingStorage contract.
#[derive(Debug, Clone)]
pub struct NodeChallenge {
    pub knowledge_collection_id: U256,
    pub chunk_id: U256,
    pub knowledge_collection_storage_contract: Address,
    pub epoch: U256,
    pub active_proof_period_start_block: U256,
}

impl EvmChain {
    /// Get the active proof period status.
    pub async fn get_active_proof_period_status(
        &self,
    ) -> Result<ProofPeriodStatus, BlockchainError> {
        let result = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts
                    .random_sampling()
                    .getActiveProofPeriodStatus()
                    .call()
                    .await
            })
            .await?;

        Ok(ProofPeriodStatus {
            active_proof_period_start_block: result.activeProofPeriodStartBlock,
            is_valid: result.isValid,
        })
    }

    /// Create a new challenge for this node.
    pub async fn create_challenge(&self) -> Result<(), BlockchainError> {
        match self
            .tx_call(|contracts| contracts.random_sampling().createChallenge())
            .await
        {
            Ok(pending_tx) => {
                self.handle_contract_call(Ok(pending_tx)).await?;
                tracing::debug!("Challenge created successfully");
                Ok(())
            }
            Err(err) => {
                // Check for "already exists" error - treat as success
                let err_str = format!("{:?}", err);
                if err_str.contains("unsolved challenge already exists") {
                    tracing::debug!("Challenge already exists for current proof period");
                    return Ok(());
                }

                tracing::warn!("Create challenge failed: {:?}", err);
                Err(BlockchainError::TransactionFailed {
                    contract: "RandomSampling".to_string(),
                    function: "createChallenge".to_string(),
                    reason: err_str,
                })
            }
        }
    }

    /// Get the current challenge for a node.
    pub async fn get_node_challenge(
        &self,
        identity_id: u128,
    ) -> Result<NodeChallenge, BlockchainError> {
        use alloy::primitives::Uint;

        let result = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts
                    .random_sampling_storage()
                    .getNodeChallenge(Uint::<72, 2>::from(identity_id))
                    .call()
                    .await
            })
            .await?;

        Ok(NodeChallenge {
            knowledge_collection_id: result.knowledgeCollectionId,
            chunk_id: result.chunkId,
            knowledge_collection_storage_contract: result.knowledgeCollectionStorageContract,
            epoch: result.epoch,
            active_proof_period_start_block: result.activeProofPeriodStartBlock,
        })
    }

    /// Get the score for a node in a specific epoch and proof period.
    pub async fn get_node_epoch_proof_period_score(
        &self,
        identity_id: u128,
        epoch: U256,
        proof_period_start_block: U256,
    ) -> Result<U256, BlockchainError> {
        use alloy::primitives::Uint;

        let score = self
            .rpc_call(|| async {
                let contracts = self.contracts().await;
                contracts
                    .random_sampling_storage()
                    .getNodeEpochProofPeriodScore(
                        Uint::<72, 2>::from(identity_id),
                        epoch,
                        proof_period_start_block,
                    )
                    .call()
                    .await
            })
            .await?;

        Ok(score)
    }

    /// Submit a proof for the current challenge.
    pub async fn submit_proof(
        &self,
        chunk: &str,
        merkle_proof: &[[u8; 32]],
    ) -> Result<(), BlockchainError> {
        // Convert merkle proof to FixedBytes<32> array
        let proof_bytes: Vec<FixedBytes<32>> = merkle_proof
            .iter()
            .map(|bytes| FixedBytes::from_slice(bytes))
            .collect();

        match self
            .tx_call(|contracts| {
                contracts
                    .random_sampling()
                    .submitProof(chunk.to_string(), proof_bytes.clone())
            })
            .await
        {
            Ok(pending_tx) => {
                self.handle_contract_call(Ok(pending_tx)).await?;
                tracing::info!("Proof submitted successfully");
                Ok(())
            }
            Err(err) => {
                let raw_error = format!("{:?}", err);
                let reason = decode_contract_error(&err).unwrap_or_else(|| raw_error.clone());

                tracing::warn!(reason = %reason, raw_error = %raw_error, "Submit proof failed");
                Err(BlockchainError::TransactionFailed {
                    contract: "RandomSampling".to_string(),
                    function: "submitProof".to_string(),
                    reason,
                })
            }
        }
    }
}
