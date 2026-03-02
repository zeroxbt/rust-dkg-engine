//! Get protocol message types.

use std::sync::Arc;

use dkg_domain::{Assertion, BlockchainId, TokenIds};
use serde::{Deserialize, Serialize};

use crate::message::ProtocolResponse;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetRequestData {
    blockchain: BlockchainId,
    contract: Arc<String>,
    knowledge_collection_id: u128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    knowledge_asset_id: Option<u128>,
    ual: Arc<String>,
    token_ids: Arc<TokenIds>,
    #[serde(default)]
    include_metadata: bool,
    #[serde(rename = "paranetUAL")]
    paranet_ual: Option<Arc<String>>,
}

#[allow(clippy::too_many_arguments)]
impl GetRequestData {
    pub fn new(
        blockchain: BlockchainId,
        contract: String,
        knowledge_collection_id: u128,
        knowledge_asset_id: Option<u128>,
        ual: String,
        token_ids: TokenIds,
        include_metadata: bool,
        paranet_ual: Option<String>,
    ) -> Self {
        Self {
            blockchain,
            contract: Arc::new(contract),
            knowledge_collection_id,
            knowledge_asset_id,
            ual: Arc::new(ual),
            token_ids: Arc::new(token_ids),
            include_metadata,
            paranet_ual: paranet_ual.map(Arc::new),
        }
    }

    /// Returns the UAL.
    pub fn ual(&self) -> &str {
        self.ual.as_str()
    }

    /// Returns the token IDs.
    pub fn token_ids(&self) -> &TokenIds {
        self.token_ids.as_ref()
    }

    /// Returns a cheap shared clone of token IDs.
    pub fn token_ids_shared(&self) -> Arc<TokenIds> {
        Arc::clone(&self.token_ids)
    }

    /// Returns whether metadata should be included.
    pub fn include_metadata(&self) -> bool {
        self.include_metadata
    }

    /// Returns the paranet UAL, if any.
    pub fn paranet_ual(&self) -> Option<&str> {
        self.paranet_ual.as_deref().map(String::as_str)
    }

    /// Returns a cheap shared clone of paranet UAL, if any.
    pub fn paranet_ual_shared(&self) -> Option<Arc<String>> {
        self.paranet_ual.as_ref().map(Arc::clone)
    }
}

/// Get ACK payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetAck {
    pub assertion: Assertion,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<String>>,
}

/// Get response data (ACK payload or error payload).
pub type GetResponseData = ProtocolResponse<GetAck>;
