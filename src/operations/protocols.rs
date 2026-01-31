pub(crate) mod publish_store {
    /// Operation name used for HTTP polling.
    ///
    /// Note: kept as "publish" for API compatibility, but it represents the
    /// store phase only (signatures), not full publish finality.
    pub(crate) const NAME: &str = "publish";
    /// Default minimum ACK responses for publish operations.
    /// The effective value is max(this, blockchain_min, user_provided).
    pub(crate) const MIN_ACK_RESPONSES: u16 = 3;
    /// Minimum number of peers required before attempting the operation.
    pub(crate) const MIN_PEERS: usize = 1;
    /// Maximum number of peers to contact for this operation.
    pub(crate) const MAX_PEERS: usize = usize::MAX;
    /// Send to all nodes at once (no batching for publish).
    pub(crate) const CONCURRENT_PEERS: usize = usize::MAX;
}

pub(crate) mod get {
    pub(crate) const NAME: &str = "get";
    pub(crate) const MIN_ACK_RESPONSES: u16 = 1;
    /// Minimum number of peers required before attempting the operation.
    pub(crate) const MIN_PEERS: usize = 1;
    /// Maximum number of peers to contact for this operation.
    pub(crate) const MAX_PEERS: usize = usize::MAX;
    pub(crate) const CONCURRENT_PEERS: usize = 5;
}

pub(crate) mod batch_get {
    /// Maximum number of UALs allowed in a single batch get request.
    pub(crate) const UAL_MAX_LIMIT: usize = 1000;
    /// Minimum number of peers required before attempting the operation.
    pub(crate) const MIN_PEERS: usize = 1;
    /// Maximum number of peers to contact for this operation.
    pub(crate) const MAX_PEERS: usize = usize::MAX;
    /// Maximum number of in-flight peer requests for this operation.
    pub(crate) const CONCURRENT_PEERS: usize = 5;
}

pub(crate) mod publish_finality {
    /// Minimum number of peers required before attempting the operation.
    pub(crate) const MIN_PEERS: usize = 1;
    /// Maximum number of peers to contact for this operation.
    pub(crate) const MAX_PEERS: usize = 1;
    /// Maximum number of in-flight peer requests for this operation.
    pub(crate) const CONCURRENT_PEERS: usize = 1;
    /// Minimum ACK responses required for success.
    pub(crate) const MIN_ACK_RESPONSES: u16 = 1;
}
