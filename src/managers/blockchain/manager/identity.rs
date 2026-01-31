use crate::{
    managers::{BlockchainManager, blockchain::error::BlockchainError},
    types::BlockchainId,
};

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
}
