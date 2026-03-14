use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Unique identifier for a blockchain network.
///
/// Format: "chaintype:chainid" (e.g., "hardhat:31337", "gnosis:100", "otp:2043").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct BlockchainId(String);

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum BlockchainIdParseError {
    #[error("invalid blockchain id '{0}': expected '<chain_type>:<chain_id>'")]
    Format(String),
    #[error("invalid blockchain id '{0}': chain type must be non-empty")]
    EmptyChainType(String),
    #[error("invalid blockchain id '{0}': chain id must be a valid u64")]
    InvalidChainId(String),
}

impl BlockchainId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parse the chain ID from the blockchain ID.
    /// E.g., "hardhat1:31337" -> 31337, "otp:2043" -> 2043
    /// Returns None if the chain ID is missing or not a valid number.
    pub fn chain_id(&self) -> Option<u64> {
        self.0
            .split_once(':')
            .and_then(|(_, chain_id)| chain_id.parse().ok())
    }
}

impl std::fmt::Display for BlockchainId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for BlockchainId {
    type Err = BlockchainIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((chain_type, chain_id)) = value.split_once(':') else {
            return Err(BlockchainIdParseError::Format(value.to_string()));
        };

        if chain_type.is_empty() {
            return Err(BlockchainIdParseError::EmptyChainType(value.to_string()));
        }

        chain_id
            .parse::<u64>()
            .map_err(|_| BlockchainIdParseError::InvalidChainId(value.to_string()))?;

        Ok(Self(value.to_string()))
    }
}

impl TryFrom<&str> for BlockchainId {
    type Error = BlockchainIdParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<String> for BlockchainId {
    type Error = BlockchainIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}
