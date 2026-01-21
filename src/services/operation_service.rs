use std::sync::Arc;

use chrono::Utc;
use repository::{OperationStatus, RepositoryManager};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use triple_store::Assertion;
use uuid::Uuid;

use crate::{error::NodeError, services::file_service::FileService};

/// Result data for a completed GET operation.
///
/// Stored in the operation result cache file when a get operation
/// completes successfully (either from local query or network responses).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetOperationResult {
    /// The retrieved assertion data (public and optionally private triples)
    pub assertion: Assertion,
    /// Optional metadata triples if requested
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<String>>,
}

impl GetOperationResult {
    /// Create a new get operation result.
    pub fn new(assertion: Assertion, metadata: Option<Vec<String>>) -> Self {
        Self {
            assertion,
            metadata,
        }
    }
}

/// Consolidated operation state service.
/// The database is the single source of truth for all operation state.
pub struct OperationService {
    repository: Arc<RepositoryManager>,
    file_service: Arc<FileService>,
    config: OperationConfig,
}

pub struct OperationConfig {
    pub operation_name: &'static str,
}

impl OperationService {
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

        tracing::debug!(operation_id = %operation_id, "Operation record created");

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

        tracing::info!(
            operation_id = %operation_id,
            total_peers = total_peers,
            min_ack = min_ack_responses,
            "[{}] Operation started",
            self.config.operation_name
        );

        Ok(())
    }

    /// Record a response and return the outcome.
    /// Uses atomic database increment to ensure each thread sees a unique count,
    /// so only one thread will trigger completion or failure.
    pub async fn record_response(
        &self,
        operation_id: Uuid,
        is_success: bool,
    ) -> Result<(), NodeError> {
        // Atomically increment the counter and get the updated record
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
            "Response recorded"
        );

        // Check if we've reached exactly the minimum replications threshold
        // Using == ensures only one thread (the one that pushed it to the threshold) triggers
        // completion
        if completed_count == min_ack_responses {
            self.repository
                .operation_repository()
                .update_status(operation_id, OperationStatus::Completed.as_str())
                .await?;

            tracing::info!(
                operation_id = %operation_id,
                completed = completed_count,
                failed = failed_count,
                total_responses = completed_count + failed_count,
                "[{}] Minimum replication reached",
                self.config.operation_name
            );
        }

        // Check if we've exhausted exactly all peers without reaching minimum
        // Using == ensures only one thread triggers failure
        if total_responses == total_peers && completed_count < min_ack_responses {
            let reason = format!(
                "Not replicated to enough nodes! Only {completed_count}/{min_ack_responses} nodes responded successfully"
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

            // Clean up cache file
            let cache_path = self
                .file_service
                .operation_result_cache_path(&operation_id.to_string());
            let _ = self.file_service.remove_file(&cache_path).await;

            tracing::warn!(
                operation_id = %operation_id,
                completed = completed_count,
                required = min_ack_responses,
                failed = failed_count,
                total_responses = completed_count + failed_count,
                "[{}] Failed - insufficient replications",
                self.config.operation_name
            );
        }

        // Still in progress
        Ok(())
    }

    /// Mark an operation as completed with a result.
    /// Used when data is found locally without needing network requests.
    pub async fn mark_completed_with_result<T: Serialize>(
        &self,
        operation_id: Uuid,
        result: &T,
    ) -> Result<(), NodeError> {
        // Store result in cache file
        let cache_dir = self.file_service.operation_result_cache_dir();
        let filename = operation_id.to_string();
        self.file_service
            .write_json(&cache_dir, &filename, result)
            .await?;

        // Update status in database
        self.repository
            .operation_repository()
            .update(
                operation_id,
                Some(OperationStatus::Completed.as_str()),
                None,
                None,
            )
            .await?;

        tracing::info!(
            operation_id = %operation_id,
            "[{}] Operation completed (local)",
            self.config.operation_name
        );

        Ok(())
    }

    /// Get a cached operation result.
    /// Returns None if the result file doesn't exist or can't be deserialized.
    pub async fn get_cached_result<T: DeserializeOwned>(
        &self,
        operation_id: Uuid,
    ) -> Option<T> {
        let cache_path = self
            .file_service
            .operation_result_cache_path(&operation_id.to_string());

        self.file_service.read_json(&cache_path).await.ok()
    }

    /// Manually fail an operation (e.g., due to external error before sending requests).
    /// This method handles errors internally by logging them, so callers don't need to
    /// handle the error case - if marking fails, there's nothing the caller can do anyway.
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

        match result {
            Ok(_) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    "[{}] Operation failed",
                    self.config.operation_name
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
