use super::{
    Multiaddr, PeerId,
    message::{RequestMessage, ResponseMessage},
    messages::{
        BatchGetAck, BatchGetRequestData, FinalityAck, FinalityRequestData, GetAck, GetRequestData,
        StoreAck, StoreRequestData,
    },
    request_response::ResponseChannel,
};

/// Handler for network events that require application-level processing.
///
/// Response events (success/failure for outbound requests) are handled
/// internally by NetworkManager via PendingRequests. This trait only
/// receives events that need to be dispatched to the application layer.
pub(crate) trait NetworkEventHandler: Send + Sync {
    // ─────────────────────────────────────────────────────────────────────────
    // Protocol inbound requests
    // ─────────────────────────────────────────────────────────────────────────

    /// Called when a store request is received from a peer.
    async fn on_store_request(
        &self,
        request: RequestMessage<StoreRequestData>,
        channel: ResponseChannel<ResponseMessage<StoreAck>>,
        peer: PeerId,
    );

    /// Called when a get request is received from a peer.
    async fn on_get_request(
        &self,
        request: RequestMessage<GetRequestData>,
        channel: ResponseChannel<ResponseMessage<GetAck>>,
        peer: PeerId,
    );

    /// Called when a finality request is received from a peer.
    async fn on_finality_request(
        &self,
        request: RequestMessage<FinalityRequestData>,
        channel: ResponseChannel<ResponseMessage<FinalityAck>>,
        peer: PeerId,
    );

    /// Called when a batch get request is received from a peer.
    async fn on_batch_get_request(
        &self,
        request: RequestMessage<BatchGetRequestData>,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        peer: PeerId,
    );

    // ─────────────────────────────────────────────────────────────────────────
    // Infrastructure events
    // ─────────────────────────────────────────────────────────────────────────

    /// Called when identify protocol receives info about a peer's listen addresses.
    async fn on_identify_received(&self, peer_id: PeerId, listen_addrs: Vec<Multiaddr>);

    /// Called when a Kademlia lookup finds the target peer.
    async fn on_kad_peer_found(&self, target: PeerId);

    /// Called when a Kademlia lookup fails to find the target peer.
    async fn on_kad_peer_not_found(&self, target: PeerId);

    /// Called when a new connection is established with a peer.
    async fn on_connection_established(&self, peer_id: PeerId);

    /// Called when a connection with a peer is closed.
    async fn on_connection_closed(&self, peer_id: PeerId);

    /// Called when this node starts listening on a new address.
    fn on_new_listen_addr(&self, address: Multiaddr);
}
