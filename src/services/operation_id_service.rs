use std::collections::HashMap;
use std::fmt::{self, Display};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use blockchain::BlockchainName;
use repository::{models::operation_ids::Model as OperationIdRecord, RepositoryManager};

use crate::error::{NodeError, ServiceError};
use crate::services::file_service::{FileService, FileServiceError};

/// Operation ID newtype wrapper for type safety and encapsulation.
/// Internally uses UUID v4, but could be changed to other ID schemes without affecting API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationId(Uuid);

impl OperationId {
    /// Generate a new operation ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Get the inner UUID (useful for database operations)
    pub fn into_inner(self) -> Uuid {
        self.0
    }
}

impl Default for OperationId {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for OperationId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

impl From<Uuid> for OperationId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<OperationId> for Uuid {
    fn from(id: OperationId) -> Self {
        id.0
    }
}

type Result<T> = std::result::Result<T, NodeError>;

// TODO: Consider adding event emitter for telemetry

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedOperationData {
    data: serde_json::Value,
    timestamp: u64,
}

pub struct OperationIdService {
    file_service: Arc<FileService>,
    repository_manager: Arc<RepositoryManager>,
    memory_cache: Arc<RwLock<HashMap<OperationId, CachedOperationData>>>,
}

impl OperationIdService {
    pub fn new(file_service: Arc<FileService>, repository_manager: Arc<RepositoryManager>) -> Self {
        Self {
            file_service,
            repository_manager,
            memory_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Generate a new operation ID and store it in the repository
    pub async fn generate_operation_id(
        &self,
        status: &str,
        blockchain: BlockchainName,
    ) -> Result<OperationId> {
        let operation_id = OperationId::new();
        let timestamp = current_timestamp();

        // Create the operation record in the repository
        self.repository_manager
            .operation_id_repository()
            .create(operation_id.into_inner(), status, timestamp as i64)
            .await
            .map_err(NodeError::Repository)?;

        tracing::debug!(
            "Generated operation id {} for blockchain {:?} with status {}",
            operation_id,
            blockchain,
            status
        );

        Ok(operation_id)
    }

    /// Get an operation ID record from the repository
    pub async fn get_operation_id_record(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<OperationIdRecord>> {
        self.repository_manager
            .operation_id_repository()
            .get(operation_id.into_inner())
            .await
            .map_err(NodeError::Repository)
    }

    /// Update operation ID status with optional error information
    pub async fn update_operation_id_status(
        &self,
        operation_id: OperationId,
        status: &str, // TODO: make this enum
    ) -> Result<()> {
        let timestamp = current_timestamp();

        self.repository_manager
            .operation_id_repository()
            .update(
                operation_id.into_inner(),
                Some(status),
                None,
                Some(timestamp as i64),
            )
            .await
            .map_err(NodeError::Repository)?;

        // TODO: remove cache on error
        /*  if error.is_some() {
            self.remove_operation_id_cache(operation_id).await?;


            this.logger.debug(`Marking operation id ${operationId} as failed`);
            response.data = JSON.stringify({ errorMessage, errorType });
            await this.removeOperationIdCache(operationId);

        } */

        Ok(())
    }

    /// Cache operation data in memory
    pub async fn cache_operation_id_data_to_memory(
        &self,
        operation_id: OperationId,
        data: serde_json::Value,
    ) -> Result<()> {
        tracing::debug!("Caching data for operation id {} in memory", operation_id);

        let cached = CachedOperationData {
            data,
            timestamp: current_timestamp(),
        };

        let mut cache = self.memory_cache.write().await;
        cache.insert(operation_id, cached);

        Ok(())
    }

    /// Cache operation data to file
    pub async fn cache_operation_id_data_to_file(
        &self,
        operation_id: OperationId,
        data: &serde_json::Value,
    ) -> Result<()> {
        tracing::debug!("Caching data for operation id {} in file", operation_id);

        let cache_dir = self.file_service.operation_id_cache_dir();
        self.file_service
            .write_json(&cache_dir, &operation_id.to_string(), data)
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        Ok(())
    }

    /// Get cached operation data from memory or file
    pub async fn get_cached_operation_id_data(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<serde_json::Value>> {
        // Try memory cache first
        {
            let cache = self.memory_cache.read().await;
            if let Some(cached) = cache.get(&operation_id) {
                tracing::debug!(
                    "Reading operation id {} cached data from memory",
                    operation_id
                );
                return Ok(Some(cached.data.clone()));
            }
        }

        // Try file cache
        tracing::debug!("Didn't find {} in memory cache, trying file", operation_id);

        let file_path = self
            .file_service
            .operation_id_path(&operation_id.to_string());

        match self.file_service.read_json(&file_path).await {
            Ok(data) => Ok(Some(data)),
            Err(FileServiceError::FileNotFound(_)) => Ok(None),
            Err(e) => Err(NodeError::Service(ServiceError::Other(e.to_string()))),
        }
    }

    /// Remove operation ID cache from both memory and file
    pub async fn remove_operation_id_file_cache(&self, operation_id: OperationId) -> Result<()> {
        tracing::debug!("Removing operation id {} cached data", operation_id);

        // Remove from file
        let file_path = self
            .file_service
            .operation_id_path(&operation_id.to_string());
        self.file_service
            .remove_file(&file_path)
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        Ok(())
    }

    /// Remove operation ID cache from memory only
    pub async fn remove_operation_id_memory_cache(&self, operation_id: OperationId) {
        tracing::debug!(
            "Removing operation id {} cached data from memory",
            operation_id
        );
        let mut cache = self.memory_cache.write().await;
        cache.remove(&operation_id);
    }

    /// Remove expired operation ID data from memory cache
    /// Returns the approximate number of bytes freed
    pub async fn remove_expired_operation_id_memory_cache(
        &self,
        expired_timeout_ms: u64,
    ) -> Result<usize> {
        let now = current_timestamp();
        let mut cache = self.memory_cache.write().await;
        let mut deleted_bytes = 0;

        cache.retain(|operation_id, cached| {
            let is_expired = cached.timestamp + expired_timeout_ms < now;
            if is_expired {
                // Rough estimate of bytes freed
                if let Ok(json) = serde_json::to_string(&cached.data) {
                    deleted_bytes += json.len();
                }
                tracing::trace!(
                    "Removing expired operation {} from memory cache",
                    operation_id
                );
            }
            !is_expired
        });

        if deleted_bytes > 0 {
            tracing::debug!(
                "Removed approximately {} bytes from memory cache",
                deleted_bytes
            );
        }

        Ok(deleted_bytes)
    }

    /// Remove expired operation ID data from file cache
    /// Returns the number of files deleted
    pub async fn remove_expired_operation_id_file_cache(
        &self,
        expired_timeout_ms: u64,
        batch_size: usize,
    ) -> Result<usize> {
        let cache_dir = self.file_service.operation_id_cache_dir();

        // Check if cache directory exists
        let entries = match self.file_service.read_dir(&cache_dir).await {
            Ok(e) => e,
            Err(_) => return Ok(0), // Directory doesn't exist, nothing to clean
        };

        let now = current_timestamp();
        let mut total_deleted = 0;

        // Process files in batches
        for chunk in entries.chunks(batch_size) {
            let mut tasks = Vec::new();

            for filename in chunk {
                let file_path = cache_dir.join(filename);
                let file_service = self.file_service.clone();

                let metadata = self.file_service.metadata(&file_path).await;
                let task = tokio::spawn(async move {
                    match metadata {
                        Ok(metadata) => {
                            if let Ok(modified) = metadata.modified() {
                                let modified_timestamp = modified
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis()
                                    as u64;

                                if modified_timestamp + expired_timeout_ms < now {
                                    return file_service
                                        .remove_file(&file_path)
                                        .await
                                        .unwrap_or(false);
                                }
                            }
                        }
                        Err(_) => return false,
                    }
                    false
                });

                tasks.push(task);
            }

            // Wait for batch to complete
            let results = futures::future::join_all(tasks).await;
            total_deleted += results
                .iter()
                .filter(|r| *r.as_ref().unwrap_or(&false))
                .count();
        }

        if total_deleted > 0 {
            tracing::debug!("Removed {} expired files from cache", total_deleted);
        }

        Ok(total_deleted)
    }
}

/// Get current timestamp in milliseconds since UNIX epoch
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_id_is_uuid() {
        let id = OperationId::new();
        assert_eq!(id.to_string().len(), 36);
    }

    #[test]
    fn test_operation_id_parse() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let id: OperationId = uuid_str.parse().unwrap();
        assert_eq!(id.to_string(), uuid_str);
    }
}
