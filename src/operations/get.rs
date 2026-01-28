use serde::{Deserialize, Serialize};

use crate::{
    managers::{
        network::messages::{GetRequestData, GetResponseData},
        triple_store::Assertion,
    },
    services::operation::Operation,
};

/// Result stored after successful Get operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GetOperationResult {
    /// The retrieved assertion data (public and optionally private triples)
    pub assertion: Assertion,
    /// Optional metadata triples if requested
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<String>>,
}

impl GetOperationResult {
    /// Create a new get operation result.
    pub(crate) fn new(assertion: Assertion, metadata: Option<Vec<String>>) -> Self {
        Self {
            assertion,
            metadata,
        }
    }
}

/// Get operation type implementation.
pub(crate) struct GetOperation;

impl Operation for GetOperation {
    const NAME: &'static str = "get";
    const MIN_ACK_RESPONSES: u16 = 1;
    const CONCURRENT_PEERS: usize = 5;

    type Request = GetRequestData;
    type Response = GetResponseData;
    type Result = GetOperationResult;
}
