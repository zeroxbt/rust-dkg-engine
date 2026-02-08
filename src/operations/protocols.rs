pub(crate) mod publish_store {
    /// Maximum number of in-flight peer requests for this operation.
    pub(crate) const CONCURRENT_PEERS: usize = usize::MAX;
}

pub(crate) mod get {
    /// Maximum number of in-flight peer requests for this operation.
    pub(crate) const CONCURRENT_PEERS: usize = 3;
}

pub(crate) mod batch_get {
    /// Maximum number of UALs allowed in a single batch get request.
    pub(crate) const UAL_MAX_LIMIT: usize = 1000;
    /// Maximum number of in-flight peer requests for this operation.
    pub(crate) const CONCURRENT_PEERS: usize = 3;
}
