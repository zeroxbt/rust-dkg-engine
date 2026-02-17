use libp2p::request_response::ResponseChannel;
use uuid::Uuid;

use super::{
    PeerId,
    message::{ResponseBody, ResponseMessage, ResponseMessageHeader, ResponseMessageType},
    messages::{
        BatchGetAck, BatchGetRequestData, FinalityAck, FinalityRequestData, GetAck, GetRequestData,
        StoreAck, StoreRequestData,
    },
};

/// Inbound request envelope delivered to application handlers.
pub struct InboundRequest<T> {
    operation_id: Uuid,
    peer_id: PeerId,
    data: T,
}

impl<T> InboundRequest<T> {
    pub(crate) fn new(operation_id: Uuid, peer_id: PeerId, data: T) -> Self {
        Self {
            operation_id,
            peer_id,
            data,
        }
    }

    /// Returns the inbound request operation ID.
    pub fn operation_id(&self) -> Uuid {
        self.operation_id
    }

    /// Returns the remote peer ID that sent this request.
    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    /// Returns the inbound request payload.
    pub fn data(&self) -> &T {
        &self.data
    }

    /// Consumes the request and returns the payload.
    pub fn into_data(self) -> T {
        self.data
    }
}

/// Opaque handle for sending a response to an inbound request.
pub struct ResponseHandle<T> {
    channel: ResponseChannel<ResponseMessage<T>>,
}

impl<T> ResponseHandle<T> {
    pub(crate) fn new(channel: ResponseChannel<ResponseMessage<T>>) -> Self {
        Self { channel }
    }

    pub(crate) fn into_inner(self) -> ResponseChannel<ResponseMessage<T>> {
        self.channel
    }
}

/// Response that should be sent immediately by the network event loop.
pub enum ImmediateResponse<T> {
    Ack {
        handle: ResponseHandle<T>,
        operation_id: Uuid,
        data: T,
    },
    Nack {
        handle: ResponseHandle<T>,
        operation_id: Uuid,
        error_message: String,
    },
    Busy {
        handle: ResponseHandle<T>,
        operation_id: Uuid,
        error_message: String,
    },
}

impl<T> ImmediateResponse<T> {
    pub fn ack(handle: ResponseHandle<T>, operation_id: Uuid, data: T) -> Self {
        Self::Ack {
            handle,
            operation_id,
            data,
        }
    }

    pub fn nack(
        handle: ResponseHandle<T>,
        operation_id: Uuid,
        error_message: impl Into<String>,
    ) -> Self {
        Self::Nack {
            handle,
            operation_id,
            error_message: error_message.into(),
        }
    }

    pub fn busy(
        handle: ResponseHandle<T>,
        operation_id: Uuid,
        error_message: impl Into<String>,
    ) -> Self {
        Self::Busy {
            handle,
            operation_id,
            error_message: error_message.into(),
        }
    }

    pub(crate) fn into_parts(self) -> (ResponseHandle<T>, ResponseMessage<T>) {
        match self {
            Self::Ack {
                handle,
                operation_id,
                data,
            } => (
                handle,
                ResponseMessage {
                    header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Ack),
                    data: ResponseBody::ack(data),
                },
            ),
            Self::Nack {
                handle,
                operation_id,
                error_message,
            } => (
                handle,
                ResponseMessage {
                    header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Nack),
                    data: ResponseBody::error(error_message),
                },
            ),
            Self::Busy {
                handle,
                operation_id,
                error_message,
            } => (
                handle,
                ResponseMessage {
                    header: ResponseMessageHeader::new(operation_id, ResponseMessageType::Busy),
                    data: ResponseBody::error(error_message),
                },
            ),
        }
    }
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
        request: InboundRequest<StoreRequestData>,
        response: ResponseHandle<StoreAck>,
    ) -> InboundDecision<StoreAck>;

    /// Called when a get request is received from a peer.
    fn on_get_request(
        &self,
        request: InboundRequest<GetRequestData>,
        response: ResponseHandle<GetAck>,
    ) -> InboundDecision<GetAck>;

    /// Called when a finality request is received from a peer.
    fn on_finality_request(
        &self,
        request: InboundRequest<FinalityRequestData>,
        response: ResponseHandle<FinalityAck>,
    ) -> InboundDecision<FinalityAck>;

    /// Called when a batch get request is received from a peer.
    fn on_batch_get_request(
        &self,
        request: InboundRequest<BatchGetRequestData>,
        response: ResponseHandle<BatchGetAck>,
    ) -> InboundDecision<BatchGetAck>;

    // Infrastructure events are emitted via PeerEvent and handled by observers.
}
