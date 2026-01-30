use serde::{Deserialize, Serialize};

use crate::{
    managers::network::message::ResponseBody,
    types::{Assertion, BlockchainId, TokenIds},
};

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

    /// Returns the blockchain identifier.
    pub(crate) fn blockchain(&self) -> &BlockchainId {
        &self.blockchain
    }

    /// Returns the contract address.
    pub(crate) fn contract(&self) -> &str {
        &self.contract
    }

    /// Returns the knowledge collection ID.
    pub(crate) fn knowledge_collection_id(&self) -> u128 {
        self.knowledge_collection_id
    }

    /// Returns the knowledge asset ID, if any.
    pub(crate) fn knowledge_asset_id(&self) -> Option<u128> {
        self.knowledge_asset_id
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
