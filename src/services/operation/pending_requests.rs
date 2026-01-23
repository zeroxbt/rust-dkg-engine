use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::oneshot;

use crate::managers::network::request_response::OutboundRequestId;

/// Error type for pending request operations.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum RequestError {
    #[error("Request timed out")]
    Timeout,

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Peer not connected")]
    NotConnected,

    #[error("Response channel closed")]
    ChannelClosed,
}

/// Tracks pending outbound requests and their oneshot channels for response delivery.
///
/// This service enables an async request-response pattern on top of libp2p's
/// event-driven model. When a request is sent, a oneshot channel is registered
/// to receive the response. When the response (or failure) arrives via the swarm
/// event loop, it's delivered through the oneshot channel.
///
/// # Usage Flow
///
/// 1. Command sends request via NetworkManager, gets `OutboundRequestId`
/// 2. Command calls `insert()` with the request_id, gets a `oneshot::Receiver`
/// 3. Command awaits the receiver
/// 4. RpcRouter receives response/failure event, calls `complete()` with the result
/// 5. The awaiting command receives the result
///
/// # Thread Safety
///
/// All operations are thread-safe via DashMap. Multiple requests can be in flight
/// concurrently.
pub(crate) struct PendingRequests<T> {
    /// Maps request_id -> oneshot sender for delivering responses
    pending: Arc<DashMap<OutboundRequestId, oneshot::Sender<Result<T, RequestError>>>>,
}

impl<T> PendingRequests<T>
where
    T: Send + 'static,
{
    pub(crate) fn new() -> Self {
        Self {
            pending: Arc::new(DashMap::new()),
        }
    }

    /// Register a pending request and get a receiver for the response.
    ///
    /// Call this after sending the request with the `OutboundRequestId` from
    /// the network manager.
    pub(crate) fn insert(
        &self,
        request_id: OutboundRequestId,
    ) -> oneshot::Receiver<Result<T, RequestError>> {
        let (tx, rx) = oneshot::channel();
        self.pending.insert(request_id, tx);

        tracing::trace!(?request_id, "Registered pending request");

        rx
    }

    /// Complete a pending request with a successful response.
    ///
    /// Called by RpcRouter when a response arrives.
    /// Returns true if the request was found and completed, false if it was
    /// already completed or timed out.
    pub(crate) fn complete_success(&self, request_id: OutboundRequestId, response: T) -> bool {
        if let Some((_, sender)) = self.pending.remove(&request_id) {
            let _ = sender.send(Ok(response));
            tracing::trace!(?request_id, "Completed pending request with success");
            true
        } else {
            tracing::debug!(
                ?request_id,
                "No pending request found (already completed or timed out)"
            );
            false
        }
    }

    /// Complete a pending request with a failure.
    ///
    /// Called by RpcRouter when a timeout or other failure occurs.
    /// Returns true if the request was found and completed, false if it was
    /// already completed.
    pub(crate) fn complete_failure(
        &self,
        request_id: OutboundRequestId,
        error: RequestError,
    ) -> bool {
        if let Some((_, sender)) = self.pending.remove(&request_id) {
            let _ = sender.send(Err(error));
            tracing::trace!(?request_id, "Completed pending request with failure");
            true
        } else {
            tracing::debug!(
                ?request_id,
                "No pending request found for failure (already completed)"
            );
            false
        }
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
