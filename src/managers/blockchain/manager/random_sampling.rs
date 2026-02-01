use alloy::primitives::U256;

use crate::{
    managers::{
        BlockchainManager,
        blockchain::{
            chains::evm::{NodeChallenge, ProofPeriodStatus},
            error::BlockchainError,
        },
    },
    types::BlockchainId,
};

impl BlockchainManager {
    /// Get the active proof period status.
    pub(crate) async fn get_active_proof_period_status(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<ProofPeriodStatus, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_active_proof_period_status().await
    }

    /// Create a new challenge for this node.
    pub(crate) async fn create_challenge(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<(), BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.create_challenge().await
    }

    /// Get the current challenge for a node.
    pub(crate) async fn get_node_challenge(
        &self,
        blockchain: &BlockchainId,
        identity_id: u128,
    ) -> Result<NodeChallenge, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_node_challenge(identity_id).await
    }

    /// Get the score for a node in a specific epoch and proof period.
    pub(crate) async fn get_node_epoch_proof_period_score(
        &self,
        blockchain: &BlockchainId,
        identity_id: u128,
        epoch: U256,
        proof_period_start_block: U256,
    ) -> Result<U256, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_node_epoch_proof_period_score(identity_id, epoch, proof_period_start_block)
            .await
    }

    /// Submit a proof for the current challenge.
    pub(crate) async fn submit_proof(
        &self,
        blockchain: &BlockchainId,
        chunk: &str,
        merkle_proof: &[[u8; 32]],
    ) -> Result<(), BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.submit_proof(chunk, merkle_proof).await
    }
}
