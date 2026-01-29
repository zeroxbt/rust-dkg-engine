use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::{Assertion, TokenIds};

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

/// Batch get response data - uses untagged enum since JS sends flat JSON
/// Nack: {"errorMessage": "..."}
/// Ack: {"assertions": {...}, "metadata": {...}}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum BatchGetResponseData {
    #[serde(rename_all = "camelCase")]
    Nack { error_message: String },
    #[serde(rename_all = "camelCase")]
    Ack {
        /// Map of UAL -> Assertion for successfully retrieved assets.
        assertions: HashMap<String, Assertion>,
        /// Map of UAL -> metadata triples (if requested).
        #[serde(default)]
        metadata: HashMap<String, Vec<String>>,
    },
}

impl BatchGetResponseData {
    pub(crate) fn ack(
        assertions: HashMap<String, Assertion>,
        metadata: HashMap<String, Vec<String>>,
    ) -> Self {
        Self::Ack {
            assertions,
            metadata,
        }
    }

    pub(crate) fn nack(message: impl Into<String>) -> Self {
        Self::Nack {
            error_message: message.into(),
        }
    }
}
