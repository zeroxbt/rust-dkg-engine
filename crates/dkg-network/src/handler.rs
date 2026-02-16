use super::{
    PeerId,
    message::{RequestMessage, ResponseMessage},
    messages::{
        BatchGetAck, BatchGetRequestData, FinalityAck, FinalityRequestData, GetAck, GetRequestData,
        StoreAck, StoreRequestData,
    },
    request_response::ResponseChannel,
};

/// Response that should be sent immediately by the network event loop.
pub struct ImmediateResponse<T> {
    pub channel: ResponseChannel<ResponseMessage<T>>,
    pub message: ResponseMessage<T>,
}

/// Decision returned by inbound request handlers.
pub enum InboundDecision<T> {
    /// Request accepted and scheduled for async command execution.
    Scheduled,
    /// Request should be responded to immediately by the network event loop.
    RespondNow(ImmediateResponse<T>),
}

/// Handler for network events that require application-level processing.
///
/// Response events (success/failure for outbound requests) are handled
/// internally by NetworkManager via PendingRequests. This trait only
/// receives events that need to be dispatched to the application layer.
///
/// Methods are synchronous to ensure inbound network processing never blocks on
/// application-level async work.
pub trait NetworkEventHandler: Send + Sync {
    // ─────────────────────────────────────────────────────────────────────────
    // Protocol inbound requests
    // ─────────────────────────────────────────────────────────────────────────

    /// Called when a store request is received from a peer.
    fn on_store_request(
        &self,
        request: RequestMessage<StoreRequestData>,
        channel: ResponseChannel<ResponseMessage<StoreAck>>,
        peer: PeerId,
    ) -> InboundDecision<StoreAck>;

    /// Called when a get request is received from a peer.
    fn on_get_request(
        &self,
        request: RequestMessage<GetRequestData>,
        channel: ResponseChannel<ResponseMessage<GetAck>>,
        peer: PeerId,
    ) -> InboundDecision<GetAck>;

    /// Called when a finality request is received from a peer.
    fn on_finality_request(
        &self,
        request: RequestMessage<FinalityRequestData>,
        channel: ResponseChannel<ResponseMessage<FinalityAck>>,
        peer: PeerId,
    ) -> InboundDecision<FinalityAck>;

    /// Called when a batch get request is received from a peer.
    fn on_batch_get_request(
        &self,
        request: RequestMessage<BatchGetRequestData>,
        channel: ResponseChannel<ResponseMessage<BatchGetAck>>,
        peer: PeerId,
    ) -> InboundDecision<BatchGetAck>;

    // Infrastructure events are emitted via PeerEvent and handled by observers.
}
