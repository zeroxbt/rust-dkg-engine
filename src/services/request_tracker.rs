use std::sync::Arc;

use dashmap::{DashMap, DashSet};
use network::{PeerId, request_response::OutboundRequestId};
use uuid::Uuid;

/// Tracks outbound requests to map request_id -> (operation_id, peer_id).
///
/// This enables:
/// 1. Looking up operation context when receiving responses or timeouts
/// 2. Detecting late responses that arrive after a timeout was already processed
///
/// Unlike SessionStore (which is protocol-specific due to generic channel types),
/// RequestTracker only stores protocol-agnostic IDs, so a single instance can be
/// shared across all protocols.
pub struct RequestTracker {
    /// Maps request_id -> (operation_id, peer_id)
    pending_requests: Arc<DashMap<OutboundRequestId, (Uuid, PeerId)>>,
    /// Set of request_ids that have timed out (to ignore late responses)
    timed_out: Arc<DashSet<OutboundRequestId>>,
}

impl RequestTracker {
    pub fn new() -> Self {
        Self {
            pending_requests: Arc::new(DashMap::new()),
            timed_out: Arc::new(DashSet::new()),
        }
    }

    /// Track a new outbound request.
    /// Call this immediately after sending a request to associate the request_id
    /// with the operation_id and peer.
    pub fn track(&self, request_id: OutboundRequestId, operation_id: Uuid, peer: PeerId) {
        tracing::trace!(
            ?request_id,
            %operation_id,
            %peer,
            "Tracking outbound request"
        );
        self.pending_requests
            .insert(request_id, (operation_id, peer));
    }

    /// Handle a successful response.
    /// Returns the (operation_id, peer) if the request was still pending,
    /// or None if the request was already timed out (late response).
    pub fn handle_response(&self, request_id: OutboundRequestId) -> Option<(Uuid, PeerId)> {
        // Check if this request already timed out
        if self.timed_out.contains(&request_id) {
            tracing::debug!(?request_id, "Ignoring late response for timed-out request");
            // Clean up the timed_out entry since we won't see this request_id again
            self.timed_out.remove(&request_id);
            return None;
        }

        // Remove and return the pending request info
        self.pending_requests
            .remove(&request_id)
            .map(|(_, (operation_id, peer))| {
                tracing::trace!(
                    ?request_id,
                    %operation_id,
                    %peer,
                    "Request completed successfully"
                );
                (operation_id, peer)
            })
    }

    /// Handle a timeout or other outbound failure.
    /// Marks the request as timed out and returns the (operation_id, peer)
    /// so the caller can record it as a NACK.
    pub fn handle_timeout(&self, request_id: OutboundRequestId) -> Option<(Uuid, PeerId)> {
        // Mark as timed out so late responses are ignored
        self.timed_out.insert(request_id);

        // Remove and return the pending request info
        self.pending_requests
            .remove(&request_id)
            .map(|(_, (operation_id, peer))| {
                tracing::debug!(
                    ?request_id,
                    %operation_id,
                    %peer,
                    "Request timed out"
                );
                (operation_id, peer)
            })
    }

    /// Check if a request has timed out (for use before processing a response).
    pub fn is_timed_out(&self, request_id: &OutboundRequestId) -> bool {
        self.timed_out.contains(request_id)
    }

    /// Get the number of pending requests (for debugging/metrics).
    pub fn pending_count(&self) -> usize {
        self.pending_requests.len()
    }

    /// Get the number of timed-out requests awaiting late response cleanup.
    pub fn timed_out_count(&self) -> usize {
        self.timed_out.len()
    }

    /// Clean up old timed-out entries to prevent memory growth.
    /// Call this periodically if you expect many timeouts.
    /// Note: Late responses will automatically clean up their timed_out entry,
    /// so this is only needed if responses never arrive at all.
    pub fn cleanup_stale_timeouts(&self, max_entries: usize) {
        if self.timed_out.len() > max_entries {
            tracing::warn!(
                count = self.timed_out.len(),
                max = max_entries,
                "Timed-out request set exceeds limit, clearing old entries"
            );
            self.timed_out.clear();
        }
    }
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn mock_request_id() -> OutboundRequestId {
        // OutboundRequestId is created internally by libp2p, but for tests
        // we need to work around this. In real usage, this comes from send_request().
        // For now, we'll skip tests that need actual OutboundRequestId creation.
        todo!("OutboundRequestId cannot be constructed in tests")
    }

    #[test]
    fn test_new_tracker_is_empty() {
        let tracker = RequestTracker::new();
        assert_eq!(tracker.pending_count(), 0);
        assert_eq!(tracker.timed_out_count(), 0);
    }
}
