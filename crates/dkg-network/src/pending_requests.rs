//! Pending request tracking.
//!
//! Tracks outbound requests and their oneshot channels for response delivery.

use std::{collections::HashMap, time::Instant};

use libp2p::{PeerId, request_response::OutboundRequestId};
use tokio::sync::oneshot;

use super::error::NetworkError;

/// Metadata captured when a request is sent, used for outcome tracking.
pub struct RequestContext {
    pub peer_id: PeerId,
    pub protocol: &'static str,
    pub started_at: Instant,
}

struct PendingRequest<T> {
    sender: oneshot::Sender<Result<T, NetworkError>>,
    context: RequestContext,
}

/// Tracks pending outbound requests and their oneshot channels for response delivery.
///
/// This enables an async request-response pattern on top of libp2p's event-driven model.
/// When a request is sent, a oneshot channel is registered to receive the response.
/// When the response (or failure) arrives via the swarm event loop, it's delivered
/// through the oneshot channel.
///
/// # Thread Safety
///
/// This is designed for single-threaded access within the NetworkService event loop.
/// All methods take `&mut self` and use a plain `HashMap`.
pub struct PendingRequests<T> {
    /// Maps request_id -> pending request details (sender + context)
    pending: HashMap<OutboundRequestId, PendingRequest<T>>,
}

impl<T> PendingRequests<T>
where
    T: Send + 'static,
{
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    /// Register a pending request with a pre-created sender and context.
    ///
    /// This is the preferred method - the caller creates the channel and passes the sender,
    /// keeping the receiver to await the response. This avoids nested channel indirection.
    pub fn insert_sender(
        &mut self,
        request_id: OutboundRequestId,
        sender: oneshot::Sender<Result<T, NetworkError>>,
        context: RequestContext,
    ) {
        self.pending
            .insert(request_id, PendingRequest { sender, context });
        tracing::trace!(?request_id, "Registered pending request");
    }

    /// Complete a pending request with a successful response.
    ///
    /// Called by the network event handler when a response arrives.
    /// Returns the request context if found, otherwise None.
    pub fn complete_success(
        &mut self,
        request_id: OutboundRequestId,
        response: T,
    ) -> Option<RequestContext> {
        if let Some(pending) = self.pending.remove(&request_id) {
            let _ = pending.sender.send(Ok(response));
            tracing::trace!(?request_id, "Completed pending request with success");
            Some(pending.context)
        } else {
            tracing::debug!(
                ?request_id,
                "No pending request found (already completed or timed out)"
            );
            None
        }
    }

    /// Complete a pending request with a failure.
    ///
    /// Called by the network event handler when a timeout or dial failure occurs.
    /// Returns the request context if found, otherwise None.
    pub fn complete_failure(
        &mut self,
        request_id: OutboundRequestId,
        error: NetworkError,
    ) -> Option<RequestContext> {
        if let Some(pending) = self.pending.remove(&request_id) {
            let _ = pending.sender.send(Err(error));
            tracing::trace!(?request_id, "Completed pending request with failure");
            Some(pending.context)
        } else {
            tracing::debug!(
                ?request_id,
                "No pending request found for failure (already completed)"
            );
            None
        }
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }
}

impl<T> Default for PendingRequests<T>
where
    T: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
