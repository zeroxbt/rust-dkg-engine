use dkg_key_value_store::{KeyValueStoreManager, OperationResultStore, ResultStoreError};
use dkg_repository::{OperationRepository, OperationStatus};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{error::NodeError, operations::OperationKind};

/// Operation status service used for HTTP polling.
///
/// Stores a lightweight status record in SQL and the typed result in redb.
pub(crate) struct OperationStatusService<K: OperationKind> {
    operation_repository: OperationRepository,
    result_store: OperationResultStore<K::Result>,
    _marker: std::marker::PhantomData<K>,
}

impl<K> OperationStatusService<K>
where
    K: OperationKind,
    K::Result: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    const MISSING_RESULT_ERROR: &'static str = "operation result missing for completion";

    /// Create a new operation status service.
    pub(crate) fn new(
        operation_repository: OperationRepository,
        kv_store_manager: &KeyValueStoreManager,
    ) -> Self {
        let result_store = K::result_store(kv_store_manager);
        Self {
            operation_repository,
            result_store,
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a new operation status record in the database.
    pub(crate) async fn create_operation(&self, operation_id: Uuid) -> Result<(), NodeError> {
        self.operation_repository
            .create(operation_id, K::NAME, OperationStatus::InProgress)
            .await?;

        tracing::debug!(
            operation_id = %operation_id,
            "[{}] Operation record created ",
            K::NAME
        );

        Ok(())
    }

    /// Store a result in the key-value store.
    pub(crate) async fn store_result(
        &self,
        operation_id: Uuid,
        result: K::Result,
    ) -> Result<(), ResultStoreError> {
        self.result_store.store_result(operation_id, result).await?;
        tracing::debug!(
            operation_id = %operation_id,
            "[{}] Result stored",
            K::NAME
        );
        Ok(())
    }

    /// Remove a result from the key-value store.
    pub(crate) async fn remove_result(&self, operation_id: Uuid) -> Result<bool, ResultStoreError> {
        self.result_store.remove_result(operation_id).await
    }

    /// Get a cached operation result from the key-value store.
    pub(crate) async fn get_result(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<K::Result>, ResultStoreError> {
        self.result_store.get_result(operation_id).await
    }

    /// Update a result in the key-value store using a closure.
    /// If no result exists, creates one from the default value.
    /// This enables incremental updates (e.g., adding signatures one at a time).
    pub(crate) async fn update_result<F>(
        &self,
        operation_id: Uuid,
        default: K::Result,
        update_fn: F,
    ) -> Result<(), ResultStoreError>
    where
        F: FnOnce(&mut K::Result) + Send + 'static,
    {
        self.result_store
            .update_result(operation_id, default, update_fn)
            .await?;
        tracing::trace!(
            operation_id = %operation_id,
            "[{}] Result updated",
            K::NAME
        );
        Ok(())
    }

    /// Complete an operation with a stored result.
    ///
    /// This ensures the result is persisted before the status is marked as completed.
    pub(crate) async fn complete_with_result(
        &self,
        operation_id: Uuid,
        result: K::Result,
    ) -> Result<(), NodeError> {
        self.store_result(operation_id, result).await?;
        self.mark_completed(operation_id).await?;
        Ok(())
    }

    /// Mark an operation as completed, ensuring a result exists first.
    pub(crate) async fn complete(&self, operation_id: Uuid) -> Result<(), NodeError> {
        if self.get_result(operation_id).await?.is_none() {
            return Err(NodeError::Other(Self::MISSING_RESULT_ERROR.to_string()));
        }
        self.mark_completed(operation_id).await
    }

    /// Mark an operation as completed.
    /// Caller should store the result first via `store_result` before calling this.
    async fn mark_completed(&self, operation_id: Uuid) -> Result<(), NodeError> {
        self.operation_repository
            .update_status(operation_id, OperationStatus::Completed)
            .await?;

        tracing::info!(
            operation_id = %operation_id,
            "[{}] Operation marked as completed",
            K::NAME
        );

        Ok(())
    }

    /// Manually fail an operation (e.g., due to external error before sending requests).
    pub(crate) async fn mark_failed(&self, operation_id: Uuid, reason: String) {
        let result = self
            .operation_repository
            .update(operation_id, Some(OperationStatus::Failed), Some(reason))
            .await;

        let _ = self.result_store.remove_result(operation_id).await;

        match result {
            Ok(_) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    "[{}] Operation failed",
                    K::NAME
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
