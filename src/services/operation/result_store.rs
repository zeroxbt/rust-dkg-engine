use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use uuid::Uuid;

/// Table definition for operation results.
/// Key: operation_id as bytes (16 bytes for UUID)
/// Value: serialized result as bytes
const RESULTS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("operation_results");

#[derive(Error, Debug)]
pub enum ResultStoreError {
    #[error("Database error: {0}")]
    Database(#[from] redb::Error),

    #[error("Database error: {0}")]
    DatabaseCreate(#[from] redb::DatabaseError),

    #[error("Database commit error: {0}")]
    Commit(#[from] redb::CommitError),

    #[error("Database transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),

    #[error("Table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("Storage error: {0}")]
    Storage(#[from] redb::StorageError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Redb-based persistent storage for operation results.
///
/// This replaces both:
/// - JSON file storage for Get results
/// - Can optionally store Publish results (though signatures still go to MySQL)
///
/// The store is shared across all operation types via type-erased serialization.
pub struct ResultStore {
    db: Database,
}

impl ResultStore {
    /// Open or create a result store at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ResultStoreError> {
        let db = Database::create(path)?;

        // Ensure the table exists by opening it in a write transaction
        let write_txn = db.begin_write()?;
        {
            let _table = write_txn.open_table(RESULTS_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    /// Store a result for an operation.
    pub fn store<R: Serialize>(
        &self,
        operation_id: Uuid,
        result: &R,
    ) -> Result<(), ResultStoreError> {
        let key = operation_id.as_bytes();
        let value = serde_json::to_vec(result)?;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(RESULTS_TABLE)?;
            table.insert(key.as_slice(), value.as_slice())?;
        }
        write_txn.commit()?;

        tracing::debug!(
            operation_id = %operation_id,
            "Result stored in redb"
        );

        Ok(())
    }

    /// Retrieve a result for an operation.
    pub fn get<R: DeserializeOwned>(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<R>, ResultStoreError> {
        let key = operation_id.as_bytes();

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(RESULTS_TABLE)?;

        match table.get(key.as_slice())? {
            Some(value) => {
                let result: R = serde_json::from_slice(value.value())?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    /// Remove a result for an operation.
    pub fn remove(&self, operation_id: Uuid) -> Result<bool, ResultStoreError> {
        let key = operation_id.as_bytes();

        let write_txn = self.db.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(RESULTS_TABLE)?;
            table.remove(key.as_slice())?.is_some()
        };
        write_txn.commit()?;

        if removed {
            tracing::trace!(
                operation_id = %operation_id,
                "Result removed from redb"
            );
        }

        Ok(removed)
    }

    /// Check if a result exists for an operation.
    pub fn exists(&self, operation_id: Uuid) -> Result<bool, ResultStoreError> {
        let key = operation_id.as_bytes();

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(RESULTS_TABLE)?;

        Ok(table.get(key.as_slice())?.is_some())
    }

    /// Update a result using a closure, performing read-modify-write atomically.
    /// If the key doesn't exist, creates a new entry using the default value.
    pub fn update<R, F>(
        &self,
        operation_id: Uuid,
        default: R,
        update_fn: F,
    ) -> Result<(), ResultStoreError>
    where
        R: Serialize + DeserializeOwned,
        F: FnOnce(&mut R),
    {
        let key = operation_id.as_bytes();

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(RESULTS_TABLE)?;

            // Read existing value or use default
            let mut result: R = match table.get(key.as_slice())? {
                Some(value) => {
                    let bytes: &[u8] = value.value();
                    serde_json::from_slice(bytes)?
                }
                None => default,
            };

            // Apply update
            update_fn(&mut result);

            // Write back
            let value = serde_json::to_vec(&result)?;
            table.insert(key.as_slice(), value.as_slice())?;
        }
        write_txn.commit()?;

        tracing::trace!(
            operation_id = %operation_id,
            "Result updated in redb"
        );

        Ok(())
    }
}

// Implement From for NodeError integration
impl From<ResultStoreError> for crate::error::NodeError {
    fn from(err: ResultStoreError) -> Self {
        crate::error::NodeError::Other(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use tempfile::TempDir;

    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestResult {
        data: String,
        count: u32,
    }

    #[test]
    fn test_store_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let store = ResultStore::open(&db_path).unwrap();

        let op_id = Uuid::new_v4();
        let result = TestResult {
            data: "hello".to_string(),
            count: 42,
        };

        store.store(op_id, &result).unwrap();

        let retrieved: Option<TestResult> = store.get(op_id).unwrap();
        assert_eq!(retrieved, Some(result));
    }

    #[test]
    fn test_get_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let store = ResultStore::open(&db_path).unwrap();

        let op_id = Uuid::new_v4();
        let retrieved: Option<TestResult> = store.get(op_id).unwrap();
        assert!(retrieved.is_none());
    }

    #[test]
    fn test_remove() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let store = ResultStore::open(&db_path).unwrap();

        let op_id = Uuid::new_v4();
        let result = TestResult {
            data: "test".to_string(),
            count: 1,
        };

        store.store(op_id, &result).unwrap();
        assert!(store.exists(op_id).unwrap());

        let removed = store.remove(op_id).unwrap();
        assert!(removed);
        assert!(!store.exists(op_id).unwrap());

        // Removing again should return false
        let removed_again = store.remove(op_id).unwrap();
        assert!(!removed_again);
    }

    #[test]
    fn test_overwrite() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let store = ResultStore::open(&db_path).unwrap();

        let op_id = Uuid::new_v4();

        store
            .store(
                op_id,
                &TestResult {
                    data: "first".to_string(),
                    count: 1,
                },
            )
            .unwrap();

        store
            .store(
                op_id,
                &TestResult {
                    data: "second".to_string(),
                    count: 2,
                },
            )
            .unwrap();

        let retrieved: TestResult = store.get(op_id).unwrap().unwrap();
        assert_eq!(retrieved.data, "second");
        assert_eq!(retrieved.count, 2);
    }

    #[test]
    fn test_update_existing() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let store = ResultStore::open(&db_path).unwrap();

        let op_id = Uuid::new_v4();

        // Store initial value
        store
            .store(
                op_id,
                &TestResult {
                    data: "initial".to_string(),
                    count: 1,
                },
            )
            .unwrap();

        // Update incrementally
        store
            .update(
                op_id,
                TestResult {
                    data: "default".to_string(),
                    count: 0,
                },
                |result| {
                    result.count += 1;
                },
            )
            .unwrap();

        let retrieved: TestResult = store.get(op_id).unwrap().unwrap();
        assert_eq!(retrieved.data, "initial"); // Data unchanged
        assert_eq!(retrieved.count, 2); // Count incremented
    }

    #[test]
    fn test_update_creates_default() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let store = ResultStore::open(&db_path).unwrap();

        let op_id = Uuid::new_v4();

        // Update non-existent key (should create from default)
        store
            .update(
                op_id,
                TestResult {
                    data: "default".to_string(),
                    count: 0,
                },
                |result| {
                    result.count = 42;
                },
            )
            .unwrap();

        let retrieved: TestResult = store.get(op_id).unwrap().unwrap();
        assert_eq!(retrieved.data, "default"); // Default data
        assert_eq!(retrieved.count, 42); // Updated count
    }
}
