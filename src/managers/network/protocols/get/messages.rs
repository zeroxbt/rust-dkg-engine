//! Get protocol message types.

use dkg_domain::{Assertion, BlockchainId, TokenIds};
use serde::{Deserialize, Serialize};

use crate::managers::network::message::ResponseBody;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetRequestData {
    blockchain: BlockchainId,
    contract: String,
    knowledge_collection_id: u128,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    knowledge_asset_id: Option<u128>,
    ual: String,
    token_ids: TokenIds,
    include_metadata: bool,
    #[serde(rename = "paranetUAL")]
    paranet_ual: Option<String>,
}

#[allow(clippy::too_many_arguments)]
impl GetRequestData {
    pub(crate) fn new(
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
            contract,
            knowledge_collection_id,
            knowledge_asset_id,
            ual,
            token_ids,
            include_metadata,
            paranet_ual,
        }
    }

    /// Returns the UAL.
    pub(crate) fn ual(&self) -> &str {
        &self.ual
    }

    /// Returns the token IDs.
    pub(crate) fn token_ids(&self) -> &TokenIds {
        &self.token_ids
    }

    /// Returns whether metadata should be included.
    pub(crate) fn include_metadata(&self) -> bool {
        self.include_metadata
    }

    /// Returns the paranet UAL, if any.
    pub(crate) fn paranet_ual(&self) -> Option<&str> {
        self.paranet_ual.as_deref()
    }
}

/// Get ACK payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GetAck {
    pub assertion: Assertion,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<String>>,
}

/// Get response data (ACK payload or error payload).
pub(crate) type GetResponseData = ResponseBody<GetAck>;
