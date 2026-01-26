use libp2p::{Multiaddr, PeerId};
use serde::{Serialize, de::DeserializeOwned};

use crate::{controllers::rpc_controller::ProtocolRequest, managers::network::RequestMessage};

/// Trait defining an operation type and its associated types.
///
/// Each operation (Get, Publish, etc.) implements this trait to specify
/// its request/response message types and persisted result.
pub(crate) trait Operation: Send + Sync + 'static {
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

    /// Persisted result type stored after operation completion.
    /// Must be serializable for redb storage.
    type Result: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Build the protocol request for sending to a peer.
    ///
    /// This wraps the request message in the appropriate ProtocolRequest variant
    /// for the network layer. Addresses are used by libp2p to dial the peer if
    /// not already connected, ensuring requests don't hang indefinitely.
    fn build_protocol_request(
        peer: PeerId,
        addresses: Vec<Multiaddr>,
        message: RequestMessage<Self::Request>,
    ) -> ProtocolRequest;
}
