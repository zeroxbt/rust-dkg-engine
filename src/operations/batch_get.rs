use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    managers::{
        network::messages::{BatchGetRequestData, BatchGetResponseData},
        triple_store::Assertion,
    },
    services::operation::Operation,
};

/// Result stored after successful batch get operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BatchGetOperationResult {
    /// UALs that were found in the local triple store.
    pub local: Vec<String>,
    /// Map of UAL -> Assertion for assets retrieved from the network.
    pub remote: HashMap<String, Assertion>,
    /// Map of UAL -> metadata triples (if requested).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Vec<String>>,
}

impl BatchGetOperationResult {
    /// Create a new batch get operation result.
    pub(crate) fn new(
        local: Vec<String>,
        remote: HashMap<String, Assertion>,
        metadata: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            local,
            remote,
            metadata,
        }
    }

    /// Create an empty result.
    pub(crate) fn empty() -> Self {
        Self {
            local: Vec::new(),
            remote: HashMap::new(),
            metadata: HashMap::new(),
        }
    }
}

/// Batch get operation type implementation.
pub(crate) struct BatchGetOperation;

impl Operation for BatchGetOperation {
    const NAME: &'static str = "batch-get";
    const MIN_ACK_RESPONSES: u16 = 1;
    const CONCURRENT_PEERS: usize = 5;

    type Request = BatchGetRequestData;
    type Response = BatchGetResponseData;
    type Result = BatchGetOperationResult;
}
