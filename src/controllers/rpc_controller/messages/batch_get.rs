use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::managers::triple_store::{Assertion, TokenIds};

/// Maximum number of UALs allowed in a single batch get request.
pub(crate) const BATCH_GET_UAL_MAX_LIMIT: usize = 1000;

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

/// Response data for batch get protocol.
///
/// Contains assertions and metadata for each successfully retrieved UAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchGetResponseData {
    /// Map of UAL -> Assertion for successfully retrieved assets.
    assertions: HashMap<String, Assertion>,
    /// Map of UAL -> metadata triples (if requested).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, Vec<String>>,
}

impl BatchGetResponseData {
    pub(crate) fn new(
        assertions: HashMap<String, Assertion>,
        metadata: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            assertions,
            metadata,
        }
    }

    /// Returns the assertions map.
    pub(crate) fn assertions(&self) -> &HashMap<String, Assertion> {
        &self.assertions
    }

    /// Returns the metadata map.
    pub(crate) fn metadata(&self) -> &HashMap<String, Vec<String>> {
        &self.metadata
    }
}
