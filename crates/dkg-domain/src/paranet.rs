use alloy::{
    primitives::{Address, B256, U256, keccak256},
    sol_types::SolValue,
};

/// Access policy for paranet nodes (matches on-chain enum).
/// OPEN = 0: Any node can participate
/// PERMISSIONED = 1: Only approved nodes can participate
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AccessPolicy {
    Open = 0,
    Permissioned = 1,
}

impl From<u8> for AccessPolicy {
    fn from(value: u8) -> Self {
        match value {
            1 => AccessPolicy::Permissioned,
            _ => AccessPolicy::Open,
        }
    }
}

/// Locator for a knowledge collection registered in a paranet.
#[derive(Debug, Clone)]
pub struct ParanetKcLocator {
    pub knowledge_collection_storage_contract: Address,
    pub knowledge_collection_token_id: u128,
}

/// Construct a paranet ID from contract address, knowledge collection ID, and knowledge asset ID.
///
/// This matches the JavaScript implementation:
/// ```js
/// keccak256EncodePacked(['address', 'uint256', 'uint256'], [contract, kcId, kaId])
/// ```
///
/// The resulting hash is used as the unique identifier for a paranet on-chain.
pub fn construct_paranet_id(
    contract: Address,
    knowledge_collection_id: u128,
    knowledge_asset_id: u128,
) -> B256 {
    let packed = (
        contract,
        U256::from(knowledge_collection_id),
        U256::from(knowledge_asset_id),
    )
        .abi_encode_packed();
    keccak256(packed)
}

/// Construct a knowledge collection on-chain ID for paranet registration checks.
///
/// This matches the JavaScript implementation:
/// ```js
/// keccak256EncodePacked(['address', 'uint256'], [contract, kcId])
/// ```
///
/// The resulting hash is used to check if a knowledge collection is registered in a paranet.
pub fn construct_knowledge_collection_onchain_id(
    contract: Address,
    knowledge_collection_id: u128,
) -> B256 {
    let packed = (contract, U256::from(knowledge_collection_id)).abi_encode_packed();
    keccak256(packed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construct_paranet_id() {
        // Test that the function produces a deterministic hash
        let contract = Address::ZERO;
        let kc_id = 1u128;
        let ka_id = 1u128;

        let id1 = construct_paranet_id(contract, kc_id, ka_id);
        let id2 = construct_paranet_id(contract, kc_id, ka_id);

        assert_eq!(id1, id2);

        // Different inputs should produce different hashes
        let id3 = construct_paranet_id(contract, kc_id, 2u128);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_construct_knowledge_collection_onchain_id() {
        let contract = Address::ZERO;
        let kc_id = 1u128;

        let id1 = construct_knowledge_collection_onchain_id(contract, kc_id);
        let id2 = construct_knowledge_collection_onchain_id(contract, kc_id);

        assert_eq!(id1, id2);

        // Different inputs should produce different hashes
        let id3 = construct_knowledge_collection_onchain_id(contract, 2u128);
        assert_ne!(id1, id3);
    }
}
