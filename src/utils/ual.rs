use blockchain::{Address, BlockchainId};

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
