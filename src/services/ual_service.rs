use blockchain::{Address, BlockchainManager, BlockchainName};
use std::sync::Arc;

pub struct UalService {
    blockchain_manager: Arc<BlockchainManager>,
}

impl UalService {
    pub fn new(blockchain_manager: Arc<BlockchainManager>) -> Self {
        Self { blockchain_manager }
    }

    pub async fn calculate_location_keyword(
        &self,
        blockchain: &BlockchainName,
        contract: &Address,
        token_id: u64,
    ) -> Vec<u8> {
        let first_assertion_id = self
            .blockchain_manager
            .get_assertion_id_by_index(blockchain, contract, token_id, 0)
            .await
            .unwrap();

        blockchain::utils::encode_packed_keyword(contract.to_owned(), first_assertion_id).unwrap()
    }
}
