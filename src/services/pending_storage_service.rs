use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    error::{NodeError, ServiceError},
    services::file_service::FileService,
    types::models::{Assertion, OperationId},
};

#[derive(Serialize, Deserialize, Clone)]
pub struct PendingStorageData {
    dataset_root: String,
    dataset: Assertion,
    // TODO: add remote_peer_id, only if later deemed necessary
}

impl PendingStorageData {
    pub fn new(dataset_root: String, dataset: Assertion) -> Self {
        Self {
            dataset_root,
            dataset,
        }
    }

    pub fn dataset_root(&self) -> &str {
        &self.dataset_root
    }

    pub fn dataset(&self) -> &Assertion {
        &self.dataset
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
    /* async cacheDataset(operationId, datasetRoot, dataset, remotePeerId) {
        this.logger.debug(
            `Caching ${datasetRoot} dataset root, operation id: ${operationId} in file in pending storage`,
        );

        await this.fileService.writeContentsToFile(
            this.fileService.getPendingStorageCachePath(),
            operationId,
            JSON.stringify({
                merkleRoot: datasetRoot,
                assertion: dataset,
                remotePeerId,
            }),
        );
    } */

    pub async fn store_dataset(
        &self,
        operation_id: OperationId,
        dataset_root: &str,
        dataset: &Assertion,
    ) -> Result<(), NodeError> {
        tracing::debug!("Storing dataset with root: {dataset_root}, operation id: {operation_id} in pending storage");

        let dir = self.file_service.pending_storage_cache_dir();
        self.file_service
            .write_json(
                dir,
                &operation_id.to_string(),
                &PendingStorageData::new(dataset_root.to_owned(), dataset.clone()),
            )
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        Ok(())
    }

    /*
    async getCachedDataset(operationId) {
        this.logger.debug(`Retrieving cached dataset for ${operationId} from pending storage`);

        const filePath = this.fileService.getPendingStorageDocumentPath(operationId);

        try {
            const fileContents = await this.fileService.readFile(filePath, true);
            return fileContents.assertion;
        } catch (error) {
            this.logger.error(
                `Failed to retrieve or parse cached dataset for ${operationId}: ${error.message}`,
            );
            throw error;
        }
    } */

    pub async fn get_dataset(
        &self,
        operation_id: OperationId,
    ) -> Result<PendingStorageData, NodeError> {
        tracing::debug!(
            "Retrieving cached dataset for operation id: {operation_id} in pending storage"
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
                    "Failed to retrieve or parse cached dataset for operation with id: {operation_id}. Error: {e}"
                );

                Err(NodeError::Service(ServiceError::Other(e.to_string())))
            }
        }
    }
}
