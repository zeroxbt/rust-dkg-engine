use std::sync::Arc;

use blockchain::{Address, BlockchainId, BlockchainManager};

pub struct UalService {
    blockchain_manager: Arc<BlockchainManager>,
}

impl UalService {
    pub fn new(blockchain_manager: Arc<BlockchainManager>) -> Self {
        Self { blockchain_manager }
    }

    // Note: calculate_location_keyword is disabled since ContentAssetStorage is not currently in
    // use This will need to be reimplemented when KnowledgeCollectionStorage is integrated
    #[allow(dead_code)]
    pub async fn calculate_location_keyword(
        &self,
        _blockchain: &BlockchainId,
        _contract: &Address,
        _token_id: u64,
    ) -> Vec<u8> {
        // TODO: Implement using KnowledgeCollectionStorage when ready
        // let first_assertion_id = self
        //     .blockchain_manager
        //     .get_assertion_id_by_index(blockchain, contract, token_id, 0)
        //     .await
        //     .unwrap();
        // blockchain::utils::encode_packed_keyword(contract.to_owned(),
        // first_assertion_id).unwrap()
        unimplemented!("calculate_location_keyword requires KnowledgeCollectionStorage integration")
    }
}
