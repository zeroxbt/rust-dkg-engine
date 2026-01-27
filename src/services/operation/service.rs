use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::watch;
use uuid::Uuid;

use super::{result_store::{ResultStoreError, TABLE_NAME}, traits::Operation};
use crate::{
    error::NodeError,
    managers::{
        key_value_store::{KeyValueStoreManager, Table},
        network::NetworkManager,
        repository::{OperationStatus, RepositoryManager},
    },
};

/// Generic operation service that handles all shared operation logic.
///
/// This service is parameterized by an `Operation` type that defines
/// the specific request/response/state/result types for each operation.
///
/// # Ownership
/// - `Table<Op::Result>` - typed table for this operation's results
/// - Completion signals - owned here
/// - `NetworkManager` - shared reference for sending requests (with per-protocol pending requests)
pub(crate) struct OperationService<Op: Operation> {
    repository: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager>,
    result_table: Table<Op::Result>,
    completion_signals: DashMap<Uuid, watch::Sender<OperationStatus>>,
}

impl<Op: Operation> OperationService<Op> {
    /// Create a new operation service.
    pub(crate) fn new(
        repository: Arc<RepositoryManager>,
        network_manager: Arc<NetworkManager>,
        kv_store_manager: &KeyValueStoreManager,
    ) -> Result<Self, ResultStoreError> {
        let result_table = kv_store_manager.table(TABLE_NAME)?;
        Ok(Self {
            repository,
            network_manager,
            result_table,
            completion_signals: DashMap::new(),
        })
    }

    /// Create a new operation record in the database and return a completion receiver.
    ///
    /// The returned watch receiver can be used by external callers to await operation
    /// completion. The receiver will be updated when `mark_completed` or `mark_failed`
    /// is called.
    ///
    /// For callers that just want to trigger an operation without awaiting, the receiver
    /// can be dropped immediately.
    pub(crate) async fn create_operation(
        &self,
        operation_id: Uuid,
    ) -> Result<watch::Receiver<OperationStatus>, NodeError> {
        self.repository
            .operation_repository()
            .create(
                operation_id,
                Op::NAME,
                OperationStatus::InProgress.as_str(),
                Utc::now().timestamp_millis(),
            )
            .await?;

        // Create completion signal for external observers
        let (tx, rx) = watch::channel(OperationStatus::InProgress);
        self.completion_signals.insert(operation_id, tx);

        tracing::debug!(
            operation_id = %operation_id,
            "[{}] Operation record created with completion signal",
            Op::NAME
        );

        Ok(rx)
    }

    /// Store a result in the key-value store.
    pub(crate) fn store_result(
        &self,
        operation_id: Uuid,
        result: &Op::Result,
    ) -> Result<(), ResultStoreError> {
        self.result_table.store(operation_id, result)?;
        tracing::debug!(
            operation_id = %operation_id,
            "[{}] Result stored",
            Op::NAME
        );
        Ok(())
    }

    /// Get a cached operation result from the key-value store.
    pub(crate) fn get_result(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<Op::Result>, ResultStoreError> {
        Ok(self.result_table.get(operation_id)?)
    }

    /// Update a result in the key-value store using a closure.
    /// If no result exists, creates one from the default value.
    /// This enables incremental updates (e.g., adding signatures one at a time).
    pub(crate) fn update_result<F>(
        &self,
        operation_id: Uuid,
        default: Op::Result,
        update_fn: F,
    ) -> Result<(), ResultStoreError>
    where
        F: FnOnce(&mut Op::Result),
    {
        self.result_table.update(operation_id, default, update_fn)?;
        tracing::trace!(
            operation_id = %operation_id,
            "[{}] Result updated",
            Op::NAME
        );
        Ok(())
    }

    /// Manually complete an operation with the final response counts.
    /// Caller should store the result first via `store_result` before calling this.
    ///
    /// # Arguments
    /// * `operation_id` - The operation identifier
    /// * `success_count` - Number of successful responses received
    /// * `failure_count` - Number of failed responses received
    pub(crate) async fn mark_completed(
        &self,
        operation_id: Uuid,
        success_count: u16,
        failure_count: u16,
    ) -> Result<(), NodeError> {
        // Update progress counts first
        self.repository
            .operation_repository()
            .update_progress(operation_id, success_count, failure_count)
            .await?;

        // Update status in database
        self.repository
            .operation_repository()
            .update_status(operation_id, OperationStatus::Completed.as_str())
            .await?;

        // Signal completion and clean up
        self.signal_completion(operation_id, OperationStatus::Completed);
        self.completion_signals.remove(&operation_id);

        tracing::info!(
            operation_id = %operation_id,
            success_count = success_count,
            failure_count = failure_count,
            "[{}] Operation marked as completed",
            Op::NAME
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
                Some(OperationStatus::Failed.as_str()),
                Some(reason),
                None,
            )
            .await;

        // Signal failure and clean up
        self.signal_completion(operation_id, OperationStatus::Failed);
        self.completion_signals.remove(&operation_id);
        let _ = self.result_table.remove(operation_id);

        match result {
            Ok(_) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    "[{}] Operation failed",
                    Op::NAME
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

    /// Signal completion status to external observers.
    fn signal_completion(&self, operation_id: Uuid, status: OperationStatus) {
        if let Some(entry) = self.completion_signals.get(&operation_id) {
            let _ = entry.send(status);
            tracing::debug!(
                operation_id = %operation_id,
                status = ?status,
                "[{}] Signaled completion",
                Op::NAME
            );
        }
    }
}
