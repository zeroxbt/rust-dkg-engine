use std::sync::Arc;

use chrono::Utc;
use repository::RepositoryManager;
use uuid::Uuid;

use crate::{
    error::NodeError, services::file_service::FileService,
    types::models::operation::OperationStatus,
};

/// Consolidated operation state manager.
/// The database is the single source of truth for all operation state.
pub struct OperationManager {
    repository: Arc<RepositoryManager>,
    file_service: Arc<FileService>,
    config: OperationConfig,
}

pub struct OperationConfig {
    pub operation_name: &'static str,
}

/// Action to take after processing a response
#[derive(Debug, Clone)]
pub enum OperationAction {
    /// Operation completed successfully - no more work needed
    Complete,
    /// Operation failed - no more work needed
    Failed { reason: String },
    /// Still waiting for responses - continue
    Continue,
    /// Already finished (idempotent handling)
    AlreadyFinished,
}

impl OperationManager {
    pub fn new(
        repository: Arc<RepositoryManager>,
        file_service: Arc<FileService>,
        config: OperationConfig,
    ) -> Self {
        Self {
            repository,
            file_service,
            config,
        }
    }

    /// Create a new operation record in the database.
    /// Call this early so any errors are visible in DB with error messages.
    pub async fn create_operation(&self, operation_id: Uuid) -> Result<(), NodeError> {
        self.repository
            .operation_repository()
            .create(
                operation_id,
                self.config.operation_name,
                OperationStatus::InProgress.as_str(),
                Utc::now().timestamp_millis(),
            )
            .await?;

        tracing::debug!("Created operation record {operation_id}");

        Ok(())
    }

    /// Initialize progress tracking for an existing operation.
    /// Call this once you know the peer count and min_ack_responses (after finding nodes).
    pub async fn initialize_progress(
        &self,
        operation_id: Uuid,
        total_peers: u16,
        min_ack_responses: u16,
    ) -> Result<(), NodeError> {
        self.repository
            .operation_repository()
            .initialize_progress(operation_id, total_peers, min_ack_responses)
            .await?;

        tracing::debug!(
            "Initialized progress for operation {operation_id}: total_peers={total_peers}, min_ack={min_ack_responses}"
        );

        Ok(())
    }

    /// Record a response and return what action to take next.
    /// Uses atomic database operations to avoid race conditions when multiple responses arrive
    /// concurrently.
    pub async fn record_response(
        &self,
        operation_id: Uuid,
        is_success: bool,
    ) -> Result<OperationAction, NodeError> {
        // Atomically increment the counter and get the updated record
        // This avoids the read-modify-write race condition
        let record = self
            .repository
            .operation_repository()
            .atomic_increment_response(operation_id, is_success)
            .await?;

        let status = OperationStatus::from_str(&record.status);

        // If already finished, return early (late response after completion)
        if status != OperationStatus::InProgress {
            tracing::trace!("Operation {operation_id} already finished, ignoring response");
            return Ok(OperationAction::AlreadyFinished);
        }

        let total_peers = record.total_peers.unwrap_or(0);
        let min_ack_responses = record.min_ack_responses.unwrap_or(0);
        let completed_count = record.completed_count;
        let failed_count = record.failed_count;
        let total_responses = completed_count + failed_count;

        tracing::trace!(
            "Operation {operation_id} response recorded: success={is_success}, \
             completed={completed_count}/{min_ack_responses}, failed={failed_count}, \
             total_peers={total_peers}"
        );

        // Check if we've reached minimum replications
        if completed_count >= min_ack_responses {
            // Use atomic compare-and-swap to mark as completed
            // Only one thread will succeed if multiple reach this point simultaneously
            return self
                .try_complete_operation(operation_id, completed_count, failed_count)
                .await;
        }

        // Check if we've exhausted all peers without reaching minimum
        if total_responses >= total_peers {
            let reason = format!(
                "Not replicated to enough nodes! Only {completed_count}/{min_ack_responses} nodes responded successfully"
            );
            return self
                .try_fail_operation(operation_id, reason, completed_count, failed_count)
                .await;
        }

        // Still in progress
        Ok(OperationAction::Continue)
    }

    /// Try to mark operation as completed using atomic compare-and-swap.
    /// Returns AlreadyFinished if another thread already completed/failed the operation.
    async fn try_complete_operation(
        &self,
        operation_id: Uuid,
        completed_count: u16,
        failed_count: u16,
    ) -> Result<OperationAction, NodeError> {
        // Atomically update status only if still in_progress
        let result = self
            .repository
            .operation_repository()
            .atomic_complete_if_in_progress(operation_id, OperationStatus::Completed.as_str(), None)
            .await?;

        match result {
            Some(_) => {
                // We won the race - log success
                let total_responses = completed_count + failed_count;
                tracing::info!(
                    "[PUBLISH] Minimum replication reached for operation: {operation_id}, \
                     completed: {completed_count}"
                );
                tracing::info!(
                    "Total number of responses: {total_responses}, \
                     failed: {failed_count}, completed: {completed_count}"
                );
                Ok(OperationAction::Complete)
            }
            None => {
                // Another thread already finished this operation
                tracing::trace!("Operation {operation_id} was already finished by another thread");
                Ok(OperationAction::AlreadyFinished)
            }
        }
    }

    /// Try to mark operation as failed using atomic compare-and-swap.
    /// Returns AlreadyFinished if another thread already completed/failed the operation.
    async fn try_fail_operation(
        &self,
        operation_id: Uuid,
        reason: String,
        completed_count: u16,
        failed_count: u16,
    ) -> Result<OperationAction, NodeError> {
        // Atomically update status only if still in_progress
        let result = self
            .repository
            .operation_repository()
            .atomic_complete_if_in_progress(
                operation_id,
                OperationStatus::Failed.as_str(),
                Some(reason.clone()),
            )
            .await?;

        match result {
            Some(_) => {
                // We won the race - clean up and log failure
                let cache_path = self
                    .file_service
                    .operation_result_cache_path(&operation_id.to_string());
                let _ = self.file_service.remove_file(&cache_path).await;

                let total_responses = completed_count + failed_count;
                tracing::warn!(
                    "[PUBLISH] Failed for operation: {operation_id}, \
                     only {completed_count} nodes responded successfully"
                );
                tracing::info!(
                    "Total number of responses: {total_responses}, \
                     failed: {failed_count}, completed: {completed_count}"
                );
                Ok(OperationAction::Failed { reason })
            }
            None => {
                // Another thread already finished this operation
                tracing::trace!("Operation {operation_id} was already finished by another thread");
                Ok(OperationAction::AlreadyFinished)
            }
        }
    }

    /// Manually fail an operation (e.g., due to external error before sending requests)
    pub async fn mark_failed(&self, operation_id: Uuid, reason: String) -> Result<(), NodeError> {
        // Get current counts from DB for logging
        let record = self
            .repository
            .operation_repository()
            .get(operation_id)
            .await?;
        let (completed, failed) = match &record {
            Some(r) => (r.completed_count, r.failed_count),
            None => (0, 0),
        };

        // Use atomic update to mark as failed
        let result = self
            .try_fail_operation(operation_id, reason, completed, failed)
            .await?;

        match result {
            OperationAction::Failed { .. } => {
                tracing::warn!(
                    "{} for operationId: {operation_id} failed.",
                    self.config.operation_name
                );
            }
            OperationAction::AlreadyFinished => {
                tracing::trace!(
                    "{} for operationId: {operation_id} was already finished.",
                    self.config.operation_name
                );
            }
            _ => {}
        }

        Ok(())
    }
}
