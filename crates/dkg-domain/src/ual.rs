use std::{fmt, str::FromStr};

use alloy::primitives::Address;
use thiserror::Error;

use crate::{BlockchainId, BlockchainIdParseError, canonical_evm_address};

#[derive(Debug, Error)]
pub enum UalParseError {
    #[error("Invalid UAL format: {0}")]
    Format(String),
    #[error("Invalid contract address: {0}")]
    Contract(String),
    #[error("Invalid blockchain identifier: {0}")]
    Blockchain(String),
    #[error("Invalid knowledge collection ID: {0}")]
    KnowledgeCollectionId(String),
    #[error("Invalid knowledge asset ID: {0}")]
    KnowledgeAssetId(String),
}

/// Parsed UAL (Universal Asset Locator) components
#[derive(Debug, Clone)]
pub struct ParsedUal {
    /// Blockchain identifier string as it appears in the UAL (e.g., "base:84532")
    pub blockchain: BlockchainId,
    /// Contract address
    pub contract: Address,
    /// Knowledge collection ID
    pub knowledge_collection_id: u128,
    /// Optional knowledge asset ID (if UAL points to a specific asset)
    pub knowledge_asset_id: Option<u128>,
}

impl ParsedUal {
    fn collection_ual(&self) -> String {
        format!(
            "did:dkg:{}/{}/{}",
            self.blockchain.as_str().to_lowercase(),
            canonical_evm_address(&self.contract),
            self.knowledge_collection_id
        )
    }

    /// Get the knowledge collection UAL (without asset ID).
    ///
    /// Example: `did:dkg:base:84532/0x1234.../123`
    pub fn knowledge_collection_ual(&self) -> String {
        Self {
            knowledge_asset_id: None,
            ..self.clone()
        }
        .to_string()
    }

    /// Get the knowledge asset UAL for a specific token ID.
    ///
    /// Example: `did:dkg:base:84532/0x1234.../123/1`
    pub fn knowledge_asset_ual(&self, token_id: u128) -> String {
        format!("{}/{}", self.knowledge_collection_ual(), token_id)
    }

    /// Convert back to full UAL string.
    ///
    /// If `knowledge_asset_id` is set, returns the asset UAL.
    /// Otherwise returns the collection UAL.
    pub fn to_ual_string(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for ParsedUal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.knowledge_asset_id {
            Some(asset_id) => write!(f, "{}/{}", self.collection_ual(), asset_id),
            None => f.write_str(&self.collection_ual()),
        }
    }
}

/// Derive a UAL string from its components
pub fn derive_ual(
    blockchain: &BlockchainId,
    contract: &Address,
    knowledge_collection_id: u128,
    knowledge_asset_id: Option<u128>,
) -> String {
    ParsedUal {
        blockchain: blockchain.clone(),
        contract: *contract,
        knowledge_collection_id,
        knowledge_asset_id,
    }
    .to_string()
}

/// Parse a UAL string into its components
///
/// UAL format: did:dkg:{blockchain}/{contract}/{knowledge_collection_id}[/{knowledge_asset_id}]
///
/// Examples:
/// - `did:dkg:base:84532/0x1234.../123` - Knowledge collection
/// - `did:dkg:base:84532/0x1234.../123/1` - Knowledge asset
pub fn parse_ual(ual: &str) -> Result<ParsedUal, UalParseError> {
    ual.parse()
}

fn parse_contract(s: &str) -> Result<Address, UalParseError> {
    s.parse()
        .map_err(|_| UalParseError::Contract(s.to_string()))
}

fn parse_knowledge_collection_id(s: &str) -> Result<u128, UalParseError> {
    s.parse()
        .map_err(|_| UalParseError::KnowledgeCollectionId(s.to_string()))
}

fn parse_knowledge_asset_id(s: &str) -> Result<u128, UalParseError> {
    s.parse()
        .map_err(|_| UalParseError::KnowledgeAssetId(s.to_string()))
}

impl From<BlockchainIdParseError> for UalParseError {
    fn from(err: BlockchainIdParseError) -> Self {
        UalParseError::Blockchain(err.to_string())
    }
}

impl FromStr for ParsedUal {
    type Err = UalParseError;

    fn from_str(ual: &str) -> Result<Self, Self::Err> {
        let stripped = ual
            .strip_prefix("did:")
            .unwrap_or(ual)
            .strip_prefix("dkg:")
            .unwrap_or(ual);

        let parts: Vec<&str> = stripped.split('/').collect();

        match parts.len() {
            4 => {
                let blockchain = parts[0].parse()?;
                let contract = parse_contract(parts[1])?;
                let knowledge_collection_id = parse_knowledge_collection_id(parts[2])?;
                let knowledge_asset_id = parse_knowledge_asset_id(parts[3])?;

                Ok(ParsedUal {
                    blockchain,
                    contract,
                    knowledge_collection_id,
                    knowledge_asset_id: Some(knowledge_asset_id),
                })
            }
            3 => {
                let blockchain = parts[0].parse()?;
                let contract = parse_contract(parts[1])?;
                let knowledge_collection_id = parse_knowledge_collection_id(parts[2])?;

                Ok(ParsedUal {
                    blockchain,
                    contract,
                    knowledge_collection_id,
                    knowledge_asset_id: None,
                })
            }
            _ => Err(UalParseError::Format(format!(
                "Expected 3 or 4 parts, got {}",
                parts.len()
            ))),
        }
    }
}
