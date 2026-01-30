use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

use super::result_store::{ResultStoreError, TABLE_NAME};
use crate::{
    error::NodeError,
    managers::{
        key_value_store::{KeyValueStoreManager, Table},
        repository::{OperationStatus, RepositoryManager},
    },
};

/// Operation status service used for HTTP polling.
///
/// Stores a lightweight status record in SQL and the typed result in redb.
pub(crate) struct OperationStatusService<R> {
    repository: Arc<RepositoryManager>,
    result_table: Table<R>,
    operation_name: &'static str,
}

impl<R> OperationStatusService<R>
where
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Create a new operation status service.
    pub(crate) fn new(
        repository: Arc<RepositoryManager>,
        kv_store_manager: &KeyValueStoreManager,
        operation_name: &'static str,
    ) -> Result<Self, ResultStoreError> {
        let result_table = kv_store_manager.table(TABLE_NAME)?;
        Ok(Self {
            repository,
            result_table,
            operation_name,
        })
    }

    /// Create a new operation status record in the database.
    pub(crate) async fn create_operation(&self, operation_id: Uuid) -> Result<(), NodeError> {
        self.repository
            .operation_repository()
            .create(
                operation_id,
                self.operation_name,
                OperationStatus::InProgress,
            )
            .await?;

        tracing::debug!(
            operation_id = %operation_id,
            "[{}] Operation record created ",
            self.operation_name
        );

        Ok(())
    }

    /// Store a result in the key-value store.
    pub(crate) fn store_result(
        &self,
        operation_id: Uuid,
        result: &R,
    ) -> Result<(), ResultStoreError> {
        self.result_table.store(operation_id, result)?;
        tracing::debug!(
            operation_id = %operation_id,
            "[{}] Result stored",
            self.operation_name
        );
        Ok(())
    }

    /// Get a cached operation result from the key-value store.
    pub(crate) fn get_result(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<R>, ResultStoreError> {
        Ok(self.result_table.get(operation_id)?)
    }

    /// Update a result in the key-value store using a closure.
    /// If no result exists, creates one from the default value.
    /// This enables incremental updates (e.g., adding signatures one at a time).
    pub(crate) fn update_result<F>(
        &self,
        operation_id: Uuid,
        default: R,
        update_fn: F,
    ) -> Result<(), ResultStoreError>
    where
        F: FnOnce(&mut R),
    {
        self.result_table.update(operation_id, default, update_fn)?;
        tracing::trace!(
            operation_id = %operation_id,
            "[{}] Result updated",
            self.operation_name
        );
        Ok(())
    }

    /// Mark an operation as completed.
    /// Caller should store the result first via `store_result` before calling this.
    pub(crate) async fn mark_completed(&self, operation_id: Uuid) -> Result<(), NodeError> {
        self.repository
            .operation_repository()
            .update_status(operation_id, OperationStatus::Completed)
            .await?;

        tracing::info!(
            operation_id = %operation_id,
            "[{}] Operation marked as completed",
            self.operation_name
        );

        Ok(())
    }

    /// Manually fail an operation (e.g., due to external error before sending requests).
    pub(crate) async fn mark_failed(&self, operation_id: Uuid, reason: String) {
        let result = self
            .repository
            .operation_repository()
            .update(
                operation_id,
                Some(OperationStatus::Failed),
                Some(reason),
            )
            .await;

        let _ = self.result_table.remove(operation_id);

        match result {
            Ok(_) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    "[{}] Operation failed",
                    self.operation_name
                );
            }
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Unable to mark operation as failed"
                );
            }
        }
    }
}
