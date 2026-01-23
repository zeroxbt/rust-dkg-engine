use libp2p::PeerId;
use network::RequestMessage;
use serde::{Serialize, de::DeserializeOwned};

use crate::controllers::rpc_controller::ProtocolRequest;

/// Trait defining an operation type and its associated types.
///
/// Each operation (Get, Publish, etc.) implements this trait to specify
/// its request/response message types, in-memory state, and persisted result.
pub trait Operation: Send + Sync + 'static {
    /// Human-readable name for logging and database records.
    const NAME: &'static str;

    /// Minimum number of ACK responses required for operation success.
    const MIN_ACK_RESPONSES: u16;

    /// Number of requests to send per batch.
    const BATCH_SIZE: usize;

    /// The request message type sent to peers.
    /// Must be Clone for batching across multiple peers.
    type Request: Clone + Send + Sync + 'static;

    /// The response message type received from peers.
    type Response: Clone + Send + Sync + 'static;

    /// In-memory state held during operation execution.
    /// Used for validation and context when processing responses.
    type State: Clone + Send + Sync + 'static;

    /// Persisted result type stored after operation completion.
    /// Must be serializable for redb storage.
    type Result: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Build the protocol request for sending to a peer.
    ///
    /// This wraps the request message in the appropriate ProtocolRequest variant
    /// for the network layer.
    fn build_protocol_request(
        peer: PeerId,
        message: RequestMessage<Self::Request>,
    ) -> ProtocolRequest;
}
