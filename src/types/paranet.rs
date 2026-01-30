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

/// A node permitted to participate in a permissioned paranet.
#[derive(Debug, Clone)]
pub(crate) struct PermissionedNode {
    /// The node's identity ID on-chain
    pub identity_id: u128,
    /// The node's peer ID as bytes
    pub node_id: Vec<u8>,
}
