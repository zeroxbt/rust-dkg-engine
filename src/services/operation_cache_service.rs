use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::error::{NodeError, ServiceError};
use crate::services::file_service::{FileService, FileServiceError};
use crate::services::operation_service::OperationId;

type Result<T> = std::result::Result<T, NodeError>;

/// Cached operation data with timestamp
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedOperationData {
    data: String,
    timestamp: u64,
}

/// Caching service for operation data (memory + file storage)
/// Provides a two-tier cache: fast memory cache with file persistence
pub struct OperationCacheService {
    file_service: Arc<FileService>,
    memory_cache: Arc<RwLock<HashMap<OperationId, CachedOperationData>>>,
}

impl OperationCacheService {
    pub fn new(file_service: Arc<FileService>) -> Self {
        Self {
            file_service,
            memory_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Cache operation data in memory
    pub async fn cache_to_memory(&self, operation_id: OperationId, data: String) -> Result<()> {
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
    pub async fn cache_to_file(&self, operation_id: OperationId, data: String) -> Result<()> {
        tracing::debug!("Caching data for operation id {} in file", operation_id);

        let cache_dir = self.file_service.operation_id_cache_dir();
        let json_value: serde_json::Value = serde_json::from_str(&data)
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        self.file_service
            .write_json(&cache_dir, &operation_id.to_string(), &json_value)
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        Ok(())
    }

    /// Get cached operation data from memory or file
    pub async fn get_cached(&self, operation_id: OperationId) -> Result<Option<String>> {
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

        match self
            .file_service
            .read_json::<serde_json::Value>(&file_path)
            .await
        {
            Ok(data) => {
                let json_str = serde_json::to_string(&data)
                    .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;
                Ok(Some(json_str))
            }
            Err(FileServiceError::FileNotFound(_)) => Ok(None),
            Err(e) => Err(NodeError::Service(ServiceError::Other(e.to_string()))),
        }
    }

    /// Remove operation cache from both memory and file
    pub async fn remove_cache(&self, operation_id: OperationId) -> Result<()> {
        tracing::debug!("Removing operation id {} cached data", operation_id);

        // Remove from memory
        {
            let mut cache = self.memory_cache.write().await;
            cache.remove(&operation_id);
        }

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

    /// Remove operation cache from memory only
    pub async fn remove_memory_cache(&self, operation_id: OperationId) {
        tracing::debug!(
            "Removing operation id {} cached data from memory",
            operation_id
        );
        let mut cache = self.memory_cache.write().await;
        cache.remove(&operation_id);
    }

    /// Remove expired operation data from memory cache
    /// Returns the approximate number of bytes freed
    pub async fn remove_expired_memory_cache(&self, expired_timeout_ms: u64) -> Result<usize> {
        let now = current_timestamp();
        let mut cache = self.memory_cache.write().await;
        let mut deleted_bytes = 0;

        cache.retain(|operation_id, cached| {
            let is_expired = cached.timestamp + expired_timeout_ms < now;
            if is_expired {
                deleted_bytes += cached.data.len();
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

    /// Remove expired operation data from file cache
    /// Returns the number of files deleted
    pub async fn remove_expired_file_cache(
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
