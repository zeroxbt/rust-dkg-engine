use alloy::primitives::Address;

use crate::{
    managers::{BlockchainManager, blockchain::error::BlockchainError},
    types::BlockchainId,
};

impl BlockchainManager {
    pub(crate) async fn get_delegators(
        &self,
        blockchain: &BlockchainId,
        identity_id: u128,
    ) -> Result<Vec<Address>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.get_delegators(identity_id).await
    }

    pub(crate) async fn has_ever_delegated_to_node(
        &self,
        blockchain: &BlockchainId,
        identity_id: u128,
        delegator: Address,
    ) -> Result<bool, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .has_ever_delegated_to_node(identity_id, delegator)
            .await
    }

    pub(crate) async fn get_last_claimed_epoch(
        &self,
        blockchain: &BlockchainId,
        identity_id: u128,
        delegator: Address,
    ) -> Result<u64, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .get_last_claimed_epoch(identity_id, delegator)
            .await
    }

    pub(crate) async fn batch_claim_delegator_rewards(
        &self,
        blockchain: &BlockchainId,
        identity_id: u128,
        epochs: &[u64],
        delegators: &[Address],
    ) -> Result<(), BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl
            .batch_claim_delegator_rewards(identity_id, epochs, delegators)
            .await
    }
}
