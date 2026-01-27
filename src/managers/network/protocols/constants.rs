use std::time::Duration;

/// Protocol timeout configuration matching the JS implementation.
///
/// These timeouts define how long to wait for a response before considering
/// a request as failed (triggering OutboundFailure::Timeout).
pub(crate) struct ProtocolTimeouts;

impl ProtocolTimeouts {
    /// Store protocol timeout (used during publish operations)
    pub(crate) const STORE: Duration = Duration::from_secs(15);

    /// Get protocol timeout
    pub(crate) const GET: Duration = Duration::from_secs(15);

    /// Finality protocol timeout
    pub(crate) const FINALITY: Duration = Duration::from_secs(60);

    /// Batch get protocol timeout
    pub(crate) const BATCH_GET: Duration = Duration::from_secs(10);

    // Future protocols (not yet implemented in Rust):
    // UPDATE: 60 seconds
    // ASK: 60 seconds
}
