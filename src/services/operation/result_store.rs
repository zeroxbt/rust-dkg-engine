use key_value_store::KeyValueStoreError;
use thiserror::Error;

/// Table name for operation results.
pub(super) const TABLE_NAME: &str = "operation_results";

#[derive(Error, Debug)]
pub enum ResultStoreError {
    #[error("Key-value store error: {0}")]
    Store(#[from] KeyValueStoreError),
}

impl From<ResultStoreError> for crate::error::NodeError {
    fn from(err: ResultStoreError) -> Self {
        crate::error::NodeError::Other(err.to_string())
    }
}
