use dkg_domain::BlockchainId;

use crate::managers::{BlockchainManager, blockchain::error::BlockchainError};

impl BlockchainManager {
    pub(crate) fn identity_id(&self, blockchain: &BlockchainId) -> u128 {
        let blockchain_impl = self.chain(blockchain).expect("Blockchain not initialized");
        blockchain_impl
            .identity_id()
            .expect("Identity ID not initialized")
    }

    pub(crate) async fn initialize_identities(
        &mut self,
        peer_id: &str,
    ) -> Result<(), BlockchainError> {
        for blockchain in self.blockchains.values_mut() {
            blockchain.initialize_identity(peer_id).await?;
        }

        Ok(())
    }

    pub(crate) async fn set_ask(
        &self,
        blockchain: &BlockchainId,
        ask_wei: u128,
    ) -> Result<(), BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        blockchain_impl.set_ask(ask_wei).await
    }
}
