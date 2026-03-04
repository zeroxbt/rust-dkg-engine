use std::collections::HashMap;

use dkg_domain::BlockchainId;
use tokio::sync::RwLock;

#[derive(Default)]
pub(crate) struct ParameterCache {
    minimum_required_signatures: RwLock<HashMap<BlockchainId, u64>>,
}

impl ParameterCache {
    pub(crate) async fn get_minimum_required_signatures(
        &self,
        blockchain: &BlockchainId,
    ) -> Option<u64> {
        self.minimum_required_signatures
            .read()
            .await
            .get(blockchain)
            .copied()
    }

    pub(crate) async fn set_minimum_required_signatures(
        &self,
        blockchain: &BlockchainId,
        value: u64,
    ) {
        self.minimum_required_signatures
            .write()
            .await
            .insert(blockchain.clone(), value);
    }

    pub(crate) async fn invalidate_minimum_required_signatures(&self, blockchain: &BlockchainId) {
        self.minimum_required_signatures
            .write()
            .await
            .remove(blockchain);
    }
}
