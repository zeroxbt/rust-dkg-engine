use alloy::primitives::Address;

/// Access policy for paranet nodes (matches on-chain enum).
/// OPEN = 0: Any node can participate
/// PERMISSIONED = 1: Only approved nodes can participate
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum AccessPolicy {
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
pub(crate) struct ParanetKcLocator {
    pub knowledge_collection_storage_contract: Address,
    pub knowledge_collection_token_id: u128,
}
