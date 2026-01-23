use libp2p::PeerId;
use network::RequestMessage;
use serde::{Deserialize, Serialize};

use crate::{
    controllers::rpc_controller::{
        ProtocolRequest,
        messages::{StoreRequestData, StoreResponseData},
    },
    services::operation::Operation,
};

/// Signature data stored after Publish operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignatureData {
    pub identity_id: String,
    pub v: u8,
    pub r: String,
    pub s: String,
    pub vs: String,
}

impl SignatureData {
    pub fn new(identity_id: String, v: u8, r: String, s: String, vs: String) -> Self {
        Self {
            identity_id,
            v,
            r,
            s,
            vs,
        }
    }
}

/// Result stored after successful Publish operation.
///
/// Contains all signatures collected during the publish operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishOperationResult {
    /// The publisher's own signature over the dataset
    pub publisher_signature: Option<SignatureData>,
    /// Signatures from network nodes that stored the dataset
    pub network_signatures: Vec<SignatureData>,
}

impl PublishOperationResult {
    pub fn new(
        publisher_signature: Option<SignatureData>,
        network_signatures: Vec<SignatureData>,
    ) -> Self {
        Self {
            publisher_signature,
            network_signatures,
        }
    }
}

/// In-memory state during Publish operation.
/// Note: Signatures are stored directly to redb, so this state is minimal.
#[derive(Debug, Clone, Default)]
pub struct PublishOperationState;

/// Publish operation type implementation.
///
/// For publish operations, `min_ack_responses` is typically configurable
/// based on network requirements. The default config uses a placeholder
/// value that should be overridden when creating the operation.
pub struct PublishOperation;

impl Operation for PublishOperation {
    const NAME: &'static str = "publish";
    /// Default minimum ACK responses for publish operations.
    /// The effective value is max(this, blockchain_min, user_provided).
    const MIN_ACK_RESPONSES: u16 = 3;
    /// Send to all nodes at once (no batching for publish).
    const BATCH_SIZE: usize = usize::MAX;

    type Request = StoreRequestData;
    type Response = StoreResponseData;
    type State = PublishOperationState;
    type Result = PublishOperationResult;

    fn build_protocol_request(
        peer: PeerId,
        message: RequestMessage<Self::Request>,
    ) -> ProtocolRequest {
        ProtocolRequest::Store { peer, message }
    }
}
