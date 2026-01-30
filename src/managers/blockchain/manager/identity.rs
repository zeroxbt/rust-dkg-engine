use crate::managers::blockchain::*;

impl BlockchainManager {
    pub(crate) async fn get_identity_id(
        &self,
        blockchain: &BlockchainId,
    ) -> Result<Option<u128>, BlockchainError> {
        let blockchain_impl = self.chain(blockchain)?;
        Ok(blockchain_impl.get_identity_id().await)
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
