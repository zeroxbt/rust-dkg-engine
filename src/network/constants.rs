use std::time::Duration;

/// Protocol timeout configuration matching the JS implementation.
///
/// These timeouts define how long to wait for a response before considering
/// a request as failed (triggering OutboundFailure::Timeout).
pub struct ProtocolTimeouts;

impl ProtocolTimeouts {
    /// Store protocol timeout (used during publish operations)
    /// JS: PUBLISH.REQUEST = 15 * 1000
    pub const STORE: Duration = Duration::from_secs(5);

    /// Get protocol timeout
    /// JS: GET.REQUEST = 15 * 1000
    pub const GET: Duration = Duration::from_secs(15);

    /// Finality protocol timeout
    /// JS: FINALITY.REQUEST = 60 * 1000
    pub const FINALITY: Duration = Duration::from_secs(60);

    // Future protocols (not yet implemented in Rust):
    // UPDATE: 60 seconds
    // ASK: 60 seconds
    // BATCH_GET: 30 seconds
}
