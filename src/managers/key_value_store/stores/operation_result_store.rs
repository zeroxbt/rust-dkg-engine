use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use uuid::Uuid;

use crate::managers::key_value_store::{KeyValueStoreError, KeyValueStoreManager, Table};

/// Table name for operation results.
const TABLE_NAME: &str = "operation_results";

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

/// Store for operation results (typed, JSON-serialized).
pub(crate) struct OperationResultStore<R> {
    table: Table<R>,
}

impl<R> OperationResultStore<R>
where
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn new(
        kv_store_manager: &KeyValueStoreManager,
    ) -> Result<Self, ResultStoreError> {
        let table = kv_store_manager.table(TABLE_NAME)?;
        Ok(Self { table })
    }

    pub(crate) fn store_result(&self, operation_id: Uuid, result: &R) -> Result<(), ResultStoreError> {
        self.table.store(operation_id.as_bytes(), result)?;
        Ok(())
    }

    pub(crate) fn remove_result(&self, operation_id: Uuid) -> Result<bool, ResultStoreError> {
        Ok(self.table.remove(operation_id.as_bytes())?)
    }

    pub(crate) fn get_result(&self, operation_id: Uuid) -> Result<Option<R>, ResultStoreError> {
        Ok(self.table.get(operation_id.as_bytes())?)
    }

    pub(crate) fn update_result<F>(
        &self,
        operation_id: Uuid,
        default: R,
        update_fn: F,
    ) -> Result<(), ResultStoreError>
    where
        F: FnOnce(&mut R),
    {
        self.table
            .update(operation_id.as_bytes(), default, update_fn)?;
        Ok(())
    }
}
