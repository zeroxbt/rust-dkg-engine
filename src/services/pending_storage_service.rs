use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    error::NodeError,
    managers::{
        key_value_store::{KeyValueStoreError, KeyValueStoreManager, Table},
        triple_store::Assertion,
    },
};

/// Table name for pending storage data.
const TABLE_NAME: &str = "pending_storage";

#[derive(Error, Debug)]
pub(crate) enum PendingStorageError {
    #[error("Key-value store error: {0}")]
    Store(#[from] KeyValueStoreError),

    #[error("Not found: {0}")]
    NotFound(Uuid),
}

impl From<PendingStorageError> for NodeError {
    fn from(err: PendingStorageError) -> Self {
        NodeError::Other(err.to_string())
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct PendingStorageData {
    dataset_root: String,
    dataset: Assertion,
    publisher_peer_id: String,
}

impl PendingStorageData {
    pub(crate) fn new(dataset_root: String, dataset: Assertion, publisher_peer_id: String) -> Self {
        Self {
            dataset_root,
            dataset,
            publisher_peer_id,
        }
    }

    pub(crate) fn dataset_root(&self) -> &str {
        &self.dataset_root
    }

    pub(crate) fn dataset(&self) -> &Assertion {
        &self.dataset
    }

    pub(crate) fn publisher_peer_id(&self) -> &str {
        &self.publisher_peer_id
    }
}

/// Service for storing pending datasets awaiting finality.
pub(crate) struct PendingStorageService {
    table: Table<PendingStorageData>,
}

impl PendingStorageService {
    /// Create a new pending storage service from a key-value store manager.
    pub(crate) fn new(kv_store_manager: &KeyValueStoreManager) -> Result<Self, KeyValueStoreError> {
        let table = kv_store_manager.table(TABLE_NAME)?;
        Ok(Self { table })
    }

    /// Store a dataset pending finality confirmation.
    pub(crate) fn store_dataset(
        &self,
        operation_id: Uuid,
        dataset_root: &str,
        dataset: &Assertion,
        publisher_peer_id: &str,
    ) -> Result<(), PendingStorageError> {
        let data = PendingStorageData::new(
            dataset_root.to_owned(),
            dataset.clone(),
            publisher_peer_id.to_owned(),
        );

        self.table.store(operation_id, &data)?;

        tracing::debug!(
            operation_id = %operation_id,
            dataset_root = %dataset_root,
            publisher_peer_id = %publisher_peer_id,
            "Dataset stored in pending storage"
        );

        Ok(())
    }

    /// Retrieve a dataset from pending storage.
    pub(crate) fn get_dataset(
        &self,
        operation_id: Uuid,
    ) -> Result<PendingStorageData, PendingStorageError> {
        match self.table.get(operation_id)? {
            Some(data) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    "Dataset retrieved from pending storage"
                );
                Ok(data)
            }
            None => {
                tracing::error!(
                    operation_id = %operation_id,
                    "Dataset not found in pending storage"
                );
                Err(PendingStorageError::NotFound(operation_id))
            }
        }
    }

    /// Remove a dataset from pending storage (after finality is confirmed).
    pub(crate) fn remove(&self, operation_id: Uuid) -> Result<bool, PendingStorageError> {
        let removed = self.table.remove(operation_id)?;

        if removed {
            tracing::trace!(
                operation_id = %operation_id,
                "Dataset removed from pending storage"
            );
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use tempfile::TempDir;

    use super::*;
    use crate::managers::key_value_store::KeyValueStoreManager;

    fn create_test_assertion() -> Assertion {
        Assertion {
            public: vec!["<s> <p> <o> .".to_string()],
            private: None,
        }
    }

    #[test]
    fn test_store_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let kv_store_manager = KeyValueStoreManager::connect(&db_path).unwrap();
        let service = PendingStorageService::new(&kv_store_manager).unwrap();

        let op_id = Uuid::new_v4();
        let dataset_root = "0x1234";
        let dataset = create_test_assertion();
        let publisher = "peer123";

        service
            .store_dataset(op_id, dataset_root, &dataset, publisher)
            .unwrap();

        let retrieved = service.get_dataset(op_id).unwrap();
        assert_eq!(retrieved.dataset_root(), dataset_root);
        assert_eq!(retrieved.publisher_peer_id(), publisher);
        assert_eq!(retrieved.dataset().public, dataset.public);
    }

    #[test]
    fn test_get_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let kv_store_manager = KeyValueStoreManager::connect(&db_path).unwrap();
        let service = PendingStorageService::new(&kv_store_manager).unwrap();

        let op_id = Uuid::new_v4();
        let result = service.get_dataset(op_id);
        assert!(matches!(result, Err(PendingStorageError::NotFound(_))));
    }

    #[test]
    fn test_remove() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let kv_store_manager = KeyValueStoreManager::connect(&db_path).unwrap();
        let service = PendingStorageService::new(&kv_store_manager).unwrap();

        let op_id = Uuid::new_v4();
        let dataset = create_test_assertion();

        service
            .store_dataset(op_id, "0x1234", &dataset, "peer123")
            .unwrap();

        let removed = service.remove(op_id).unwrap();
        assert!(removed);

        // Should not exist anymore
        assert!(matches!(
            service.get_dataset(op_id),
            Err(PendingStorageError::NotFound(_))
        ));

        // Removing again should return false
        let removed_again = service.remove(op_id).unwrap();
        assert!(!removed_again);
    }
}
