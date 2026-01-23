use blockchain::BlockchainId;
use libp2p::PeerId;
use network::RequestMessage;
use serde::{Deserialize, Serialize};
use triple_store::{Assertion, Visibility};

use crate::{
    controllers::rpc_controller::{
        ProtocolRequest,
        messages::{GetRequestData, GetResponseData},
    },
    services::operation::Operation,
};

/// Result stored after successful Get operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetOperationResult {
    /// The retrieved assertion data (public and optionally private triples)
    pub assertion: Assertion,
    /// Optional metadata triples if requested
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<String>>,
}

impl GetOperationResult {
    /// Create a new get operation result.
    pub fn new(assertion: Assertion, metadata: Option<Vec<String>>) -> Self {
        Self {
            assertion,
            metadata,
        }
    }
}

/// In-memory state during Get operation.
/// Used for validation when processing responses.
#[derive(Debug, Clone)]
pub struct GetOperationState {
    /// The blockchain where the knowledge collection exists
    pub blockchain: BlockchainId,
    /// The knowledge collection ID
    pub knowledge_collection_id: u128,
    /// The specific knowledge asset ID (if requesting a single asset)
    pub knowledge_asset_id: Option<u128>,
    /// The visibility being requested (public, private, or all)
    pub visibility: Visibility,
}

impl GetOperationState {
    pub fn new(
        blockchain: BlockchainId,
        knowledge_collection_id: u128,
        knowledge_asset_id: Option<u128>,
        visibility: Visibility,
    ) -> Self {
        Self {
            blockchain,
            knowledge_collection_id,
            knowledge_asset_id,
            visibility,
        }
    }
}

/// Get operation type implementation.
pub struct GetOperation;

impl Operation for GetOperation {
    const NAME: &'static str = "get";
    const MIN_ACK_RESPONSES: u16 = 1;
    const BATCH_SIZE: usize = 5;

    type Request = GetRequestData;
    type Response = GetResponseData;
    type State = GetOperationState;
    type Result = GetOperationResult;

    fn build_protocol_request(
        peer: PeerId,
        message: RequestMessage<Self::Request>,
    ) -> ProtocolRequest {
        ProtocolRequest::Get { peer, message }
    }
}
