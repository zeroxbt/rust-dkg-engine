use dkg_domain::{BlockchainId, SignatureComponents};

use crate::managers::{BlockchainManager, blockchain::error::BlockchainError};

impl BlockchainManager {
    pub(crate) async fn sign_message(
        &self,
        blockchain: &BlockchainId,
        message: &str,
    ) -> Result<SignatureComponents, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.sign_message(message).await
    }

    pub(crate) async fn set_stake(
        &self,
        blockchain: &BlockchainId,
        stake_wei: u128,
    ) -> Result<(), BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.set_stake(stake_wei).await
    }
}
