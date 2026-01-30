use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    managers::network::message::ResponseBody,
    types::{Assertion, TokenIds},
};

/// Request data for batch get protocol.
///
/// Contains multiple UALs and their corresponding token IDs to retrieve
/// from the network in a single request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchGetRequestData {
    /// The blockchain network identifier.
    blockchain: String,
    /// List of UALs to retrieve.
    uals: Vec<String>,
    /// Token IDs for each UAL (keyed by UAL string).
    token_ids: HashMap<String, TokenIds>,
    /// Whether to include metadata in the response.
    include_metadata: bool,
}

impl BatchGetRequestData {
    pub(crate) fn new(
        blockchain: String,
        uals: Vec<String>,
        token_ids: HashMap<String, TokenIds>,
        include_metadata: bool,
    ) -> Self {
        Self {
            blockchain,
            uals,
            token_ids,
            include_metadata,
        }
    }

    /// Returns the list of UALs.
    pub(crate) fn uals(&self) -> &[String] {
        &self.uals
    }

    /// Returns the token IDs map.
    pub(crate) fn token_ids(&self) -> &HashMap<String, TokenIds> {
        &self.token_ids
    }

    /// Returns whether metadata should be included.
    pub(crate) fn include_metadata(&self) -> bool {
        self.include_metadata
    }
}

/// Batch get ACK payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchGetAck {
    /// Map of UAL -> Assertion for successfully retrieved assets.
    pub assertions: HashMap<String, Assertion>,
    /// Map of UAL -> metadata triples (if requested).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Vec<String>>,
}

/// Batch get response data (ACK payload or error payload).
pub(crate) type BatchGetResponseData = ResponseBody<BatchGetAck>;
