use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct KeyValueStoreManagerConfig {
    /// Maximum concurrent operations.
    /// Limits how many key-value store operations can run simultaneously.
    pub max_concurrent_operations: usize,
}
