use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use triple_store::Assertion;

use crate::{
    error::{NodeError, ServiceError},
    services::file_service::FileService,
};

#[derive(Serialize, Deserialize, Clone)]
pub struct PendingStorageData {
    dataset_root: String,
    dataset: Assertion,
    publisher_peer_id: String,
}

impl PendingStorageData {
    pub fn new(dataset_root: String, dataset: Assertion, publisher_peer_id: String) -> Self {
        Self {
            dataset_root,
            dataset,
            publisher_peer_id,
        }
    }

    pub fn dataset_root(&self) -> &str {
        &self.dataset_root
    }

    pub fn dataset(&self) -> &Assertion {
        &self.dataset
    }

    pub fn publisher_peer_id(&self) -> &str {
        &self.publisher_peer_id
    }
}

pub struct PendingStorageService {
    file_service: Arc<FileService>,
}

impl PendingStorageService {
    pub fn new(file_service: Arc<FileService>) -> Self {
        Self { file_service }
    }
}

impl PendingStorageService {
    pub async fn store_dataset(
        &self,
        operation_id: Uuid,
        dataset_root: &str,
        dataset: &Assertion,
        publisher_peer_id: &str,
    ) -> Result<(), NodeError> {
        tracing::debug!(
            operation_id = %operation_id,
            dataset_root = %dataset_root,
            publisher_peer_id = %publisher_peer_id,
            "Storing dataset in pending storage"
        );

        let dir = self.file_service.pending_storage_cache_dir();
        self.file_service
            .write_json(
                dir,
                &operation_id.to_string(),
                &PendingStorageData::new(
                    dataset_root.to_owned(),
                    dataset.clone(),
                    publisher_peer_id.to_owned(),
                ),
            )
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        Ok(())
    }

    pub async fn get_dataset(&self, operation_id: Uuid) -> Result<PendingStorageData, NodeError> {
        tracing::debug!(
            operation_id = %operation_id,
            "Retrieving dataset from pending storage"
        );

        let file_path = self
            .file_service
            .pending_storage_path(&operation_id.to_string());

        match self
            .file_service
            .read_json::<PendingStorageData>(file_path)
            .await
        {
            Ok(data) => Ok(data),
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to retrieve cached dataset"
                );

                Err(NodeError::Service(ServiceError::Other(e.to_string())))
            }
        }
    }
}
