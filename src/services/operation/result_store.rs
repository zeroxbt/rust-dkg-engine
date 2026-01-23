use thiserror::Error;

use crate::managers::key_value_store::KeyValueStoreError;

/// Table name for operation results.
pub(super) const TABLE_NAME: &str = "operation_results";

#[derive(Error, Debug)]
pub(crate) enum ResultStoreError {
    #[error("Key-value store error: {0}")]
    Store(#[from] KeyValueStoreError),
}

impl From<ResultStoreError> for crate::error::NodeError {
    fn from(err: ResultStoreError) -> Self {
        crate::error::NodeError::Other(err.to_string())
    }
}
