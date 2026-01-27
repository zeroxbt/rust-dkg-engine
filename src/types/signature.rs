use serde::{Deserialize, Serialize};

/// ECDSA signature components for EVM transactions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SignatureComponents {
    pub v: u8,
    pub r: String,
    pub s: String,
    pub vs: String,
}
