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
    /// Maximum number of in-flight peer requests for this operation.
    pub(crate) const CONCURRENT_PEERS: usize = usize::MAX;
}

pub(crate) mod get {
    pub(crate) const NAME: &str = "get";
    pub(crate) const MIN_ACK_RESPONSES: u16 = 1;
    /// Minimum number of peers required before attempting the operation.
    pub(crate) const MIN_PEERS: usize = 1;
    /// Maximum number of in-flight peer requests for this operation.
    pub(crate) const CONCURRENT_PEERS: usize = 5;
}

pub(crate) mod batch_get {
    /// Maximum number of UALs allowed in a single batch get request.
    pub(crate) const UAL_MAX_LIMIT: usize = 1000;
    /// Minimum number of peers required before attempting the operation.
    pub(crate) const MIN_PEERS: usize = 1;
    /// Maximum number of in-flight peer requests for this operation.
    pub(crate) const CONCURRENT_PEERS: usize = 3;
}
