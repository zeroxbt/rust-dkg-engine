use blockchain::{Address, BlockchainId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum UalParseError {
    #[error("Invalid UAL format: {0}")]
    Format(String),
    #[error("Invalid contract address: {0}")]
    Contract(String),
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
    /// Get the knowledge collection UAL (without asset ID).
    ///
    /// Example: `did:dkg:base:84532/0x1234.../123`
    pub fn knowledge_collection_ual(&self) -> String {
        format!(
            "did:dkg:{}/{:?}/{}",
            self.blockchain.as_str().to_lowercase(),
            self.contract,
            self.knowledge_collection_id
        )
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
        match self.knowledge_asset_id {
            Some(asset_id) => self.knowledge_asset_ual(asset_id),
            None => self.knowledge_collection_ual(),
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

/// Parse a UAL string into its components
///
/// UAL format: did:dkg:{blockchain}/{contract}/{knowledge_collection_id}[/{knowledge_asset_id}]
///
/// Examples:
/// - `did:dkg:base:84532/0x1234.../123` - Knowledge collection
/// - `did:dkg:base:84532/0x1234.../123/1` - Knowledge asset
pub fn parse_ual(ual: &str) -> Result<ParsedUal, UalParseError> {
    // Remove the "did:" and "dkg:" prefixes
    let stripped = ual
        .strip_prefix("did:")
        .unwrap_or(ual)
        .strip_prefix("dkg:")
        .unwrap_or(ual);

    let parts: Vec<&str> = stripped.split('/').collect();

    match parts.len() {
        // Format: blockchain/contract/knowledge_collection_id/knowledge_asset_id
        4 => {
            let blockchain = BlockchainId::from(parts[0]);
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
        // Format: blockchain/contract/knowledge_collection_id
        3 => {
            let blockchain = BlockchainId::from(parts[0]);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ual_with_asset_id() {
        let ual = "did:dkg:base:84532/0x6C1AeF3601cd0e04cD5e8E70e7ea2c11D2eF60f4/123/1";
        let parsed = parse_ual(ual).unwrap();
        assert_eq!(parsed.blockchain.as_str(), "base:84532");
        assert_eq!(parsed.knowledge_collection_id, 123);
        assert_eq!(parsed.knowledge_asset_id, Some(1));
    }

    #[test]
    fn test_parse_ual_without_asset_id() {
        let ual = "did:dkg:base:84532/0x6C1AeF3601cd0e04cD5e8E70e7ea2c11D2eF60f4/456";
        let parsed = parse_ual(ual).unwrap();
        assert_eq!(parsed.blockchain.as_str(), "base:84532");
        assert_eq!(parsed.knowledge_collection_id, 456);
        assert_eq!(parsed.knowledge_asset_id, None);
    }
}
