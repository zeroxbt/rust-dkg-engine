use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};

use crate::{
    controllers::rpc_controller::{
        ProtocolRequest,
        messages::{GetRequestData, GetResponseData},
    },
    managers::{network::RequestMessage, triple_store::Assertion},
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
    const BATCH_SIZE: usize = 5;

    type Request = GetRequestData;
    type Response = GetResponseData;
    type Result = GetOperationResult;

    fn build_protocol_request(
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        message: RequestMessage<Self::Request>,
    ) -> ProtocolRequest {
        ProtocolRequest::Get {
            peer,
            addresses,
            message,
        }
    }
}
