use std::sync::Arc;

use blockchain::{Address, BlockchainId, BlockchainManager};

pub struct UalService {
    blockchain_manager: Arc<BlockchainManager>,
}

impl UalService {
    pub fn new(blockchain_manager: Arc<BlockchainManager>) -> Self {
        Self { blockchain_manager }
    }

    /// Derives a Universal Asset Locator (UAL) from blockchain, contract, and collection/asset IDs.
    ///
    /// Format: `did:dkg:{blockchain}/{contract}/{knowledgeCollectionId}[/{knowledgeAssetId}]`
    pub fn derive_ual(
        blockchain: &BlockchainId,
        contract: &Address,
        knowledge_collection_id: u128,
        knowledge_asset_id: Option<u128>,
    ) -> String {
        let base_ual = format!(
            "did:dkg:{}/{:?}/{}",
            blockchain.as_str().to_lowercase(),
            contract,
            knowledge_collection_id
        );

        match knowledge_asset_id {
            Some(asset_id) => format!("{}/{}", base_ual, asset_id),
            None => base_ual,
        }
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
