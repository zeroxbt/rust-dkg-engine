use serde::{Deserialize, Serialize};

/// Unique identifier for a blockchain network.
///
/// Format: "chaintype:chainid" (e.g., "hardhat:31337", "gnosis:100", "otp:2043").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct BlockchainId(String);

impl BlockchainId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parse the chain ID from the blockchain ID.
    /// E.g., "hardhat1:31337" -> 31337, "otp:2043" -> 2043
    /// Returns None if the chain ID is missing or not a valid number.
    pub fn chain_id(&self) -> Option<u64> {
        self.0.split(':').nth(1).and_then(|s| s.parse().ok())
    }
}

impl std::fmt::Display for BlockchainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for BlockchainId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for BlockchainId {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}
