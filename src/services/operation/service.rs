use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use key_value_store::{KeyValueStoreManager, Table};
use repository::{OperationStatus, RepositoryManager};
use tokio::sync::watch;
use uuid::Uuid;

use super::{
    context_store::ContextStore,
    result_store::{ResultStoreError, TABLE_NAME},
    traits::Operation,
};
use crate::{error::NodeError, services::RequestTracker};

/// Generic operation service that handles all shared operation logic.
///
/// This service is parameterized by an `Operation` type that defines
/// the specific request/response/state/result types for each operation.
///
/// # Ownership
/// - `ContextStore<Op::State>` - owned here
/// - `Table<Op::Result>` - typed table for this operation's results
/// - Completion signals - owned here
/// - `RequestTracker` - shared reference
pub struct OperationService<Op: Operation> {
    repository: Arc<RepositoryManager>,
    context_store: ContextStore<Op::State>,
    result_table: Table<Op::Result>,
    completion_signals: DashMap<Uuid, watch::Sender<OperationStatus>>,
    request_tracker: Arc<RequestTracker>,
}

impl<Op: Operation> OperationService<Op> {
    /// Create a new operation service.
    pub fn new(
        repository: Arc<RepositoryManager>,
        kv_store_manager: &KeyValueStoreManager,
        request_tracker: Arc<RequestTracker>,
    ) -> Result<Self, ResultStoreError> {
        let result_table = kv_store_manager.table(TABLE_NAME)?;
        Ok(Self {
            repository,
            context_store: ContextStore::with_default_ttl(),
            result_table,
            completion_signals: DashMap::new(),
            request_tracker,
        })
    }

    /// Access the shared request tracker.
    pub fn request_tracker(&self) -> &Arc<RequestTracker> {
        &self.request_tracker
    }

    /// Create a new operation record in the database.
    pub async fn create_operation(&self, operation_id: Uuid) -> Result<(), NodeError> {
        self.repository
            .operation_repository()
            .create(
                operation_id,
                Op::NAME,
                OperationStatus::InProgress.as_str(),
                Utc::now().timestamp_millis(),
            )
            .await?;

        tracing::debug!(
            operation_id = %operation_id,
            "[{}] Operation record created",
            Op::NAME
        );

        Ok(())
    }

    /// Initialize progress tracking and create completion signal.
    /// Returns a watch receiver for the batch sender to monitor.
    pub async fn initialize_progress(
        &self,
        operation_id: Uuid,
        total_peers: u16,
        min_ack_responses: u16,
    ) -> Result<watch::Receiver<OperationStatus>, NodeError> {
        self.repository
            .operation_repository()
            .initialize_progress(operation_id, total_peers, min_ack_responses)
            .await?;

        // Create completion signal for batch sender
        let (tx, rx) = watch::channel(OperationStatus::InProgress);
        self.completion_signals.insert(operation_id, tx);

        tracing::info!(
            operation_id = %operation_id,
            total_peers = total_peers,
            min_ack = min_ack_responses,
            "[{}] Operation initialized",
            Op::NAME
        );

        Ok(rx)
    }

    /// Store operation context/state for response processing.
    pub fn store_context(&self, operation_id: Uuid, state: Op::State) {
        self.context_store.store(operation_id, state);
    }

    /// Get operation context (does not remove it).
    pub fn get_context(&self, operation_id: &Uuid) -> Option<Op::State> {
        self.context_store.get(operation_id)
    }

    /// Remove operation context.
    pub fn remove_context(&self, operation_id: &Uuid) -> Option<Op::State> {
        self.context_store.remove(operation_id)
    }

    /// Record a response and check for completion.
    /// Signals completion via watch channel when thresholds are met.
    /// Returns the completion status if the operation reached a terminal state.
    pub async fn record_response(
        &self,
        operation_id: Uuid,
        is_success: bool,
    ) -> Result<Option<OperationStatus>, NodeError> {
        let record = self
            .repository
            .operation_repository()
            .atomic_increment_response(operation_id, is_success)
            .await?;

        let total_peers = record.total_peers.unwrap_or(0);
        let min_ack_responses = record.min_ack_responses.unwrap_or(0);
        let completed_count = record.completed_count;
        let failed_count = record.failed_count;
        let total_responses = completed_count + failed_count;

        tracing::trace!(
            operation_id = %operation_id,
            success = is_success,
            completed = completed_count,
            min_ack = min_ack_responses,
            failed = failed_count,
            total_peers = total_peers,
            "[{}] Response recorded",
            Op::NAME
        );

        // Check completion threshold
        // Using == ensures only one thread (the one that pushed it to the threshold) triggers
        if completed_count == min_ack_responses {
            self.repository
                .operation_repository()
                .update_status(operation_id, OperationStatus::Completed.as_str())
                .await?;

            // Signal completion to batch sender
            self.signal_completion(operation_id, OperationStatus::Completed);

            tracing::info!(
                operation_id = %operation_id,
                completed = completed_count,
                failed = failed_count,
                "[{}] Minimum replication reached",
                Op::NAME
            );

            return Ok(Some(OperationStatus::Completed));
        }

        // Check failure (all responses received but threshold not met)
        if total_responses == total_peers && completed_count < min_ack_responses {
            let reason = format!(
                "Not replicated to enough nodes! Only {completed_count}/{min_ack_responses} \
                 nodes responded successfully"
            );
            self.repository
                .operation_repository()
                .update(
                    operation_id,
                    Some(OperationStatus::Failed.as_str()),
                    Some(reason),
                    None,
                )
                .await?;

            // Signal failure to batch sender
            self.signal_completion(operation_id, OperationStatus::Failed);

            // Clean up result if stored
            let _ = self.result_table.remove(operation_id);

            tracing::warn!(
                operation_id = %operation_id,
                completed = completed_count,
                required = min_ack_responses,
                failed = failed_count,
                "[{}] Failed - insufficient replications",
                Op::NAME
            );

            return Ok(Some(OperationStatus::Failed));
        }

        Ok(None)
    }

    /// Store a result in the key-value store.
    pub fn store_result(
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
    pub fn get_result(&self, operation_id: Uuid) -> Result<Option<Op::Result>, ResultStoreError> {
        Ok(self.result_table.get(operation_id)?)
    }

    /// Update a result in the key-value store using a closure.
    /// If no result exists, creates one from the default value.
    /// This enables incremental updates (e.g., adding signatures one at a time).
    pub fn update_result<F>(
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

    /// Manually complete an operation (e.g., for local-first completions without network requests).
    /// Caller should store the result first via `store_result` before calling this.
    pub async fn mark_completed(&self, operation_id: Uuid) -> Result<(), NodeError> {
        // Update status in database
        self.repository
            .operation_repository()
            .update_status(operation_id, OperationStatus::Completed.as_str())
            .await?;

        // Signal completion
        self.signal_completion(operation_id, OperationStatus::Completed);

        tracing::info!(
            operation_id = %operation_id,
            "[{}] Operation marked as completed",
            Op::NAME
        );

        Ok(())
    }

    /// Manually fail an operation (e.g., due to external error before sending requests).
    pub async fn mark_failed(&self, operation_id: Uuid, reason: String) {
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

        // Signal failure
        self.signal_completion(operation_id, OperationStatus::Failed);

        // Clean up
        let _ = self.result_table.remove(operation_id);
        self.remove_context(&operation_id);

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

    /// Clean up resources after operation completion.
    pub fn cleanup(&self, operation_id: &Uuid) {
        self.completion_signals.remove(operation_id);
        self.remove_context(operation_id);
    }

    /// Signal completion status to the batch sender.
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
