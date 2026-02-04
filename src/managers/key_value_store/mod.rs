mod error;

use std::{path::Path, sync::Arc};

pub(crate) use error::KeyValueStoreError;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

/// Table definition type alias for string table names.
type TableDef = TableDefinition<'static, &'static [u8], &'static [u8]>;

/// A typed table handle for a specific data type.
///
/// This provides type-safe access to a table in the key-value store,
/// with automatic serialization/deserialization of values.
pub(crate) struct Table<V> {
    db: Arc<Database>,
    table_def: TableDef,
    _marker: std::marker::PhantomData<V>,
}

impl<V: Serialize + DeserializeOwned> Table<V> {
    fn new(db: Arc<Database>, table_def: TableDef) -> Self {
        Self {
            db,
            table_def,
            _marker: std::marker::PhantomData,
        }
    }

    /// Store a value with a UUID key.
    pub(crate) fn store(&self, key: Uuid, value: &V) -> Result<(), KeyValueStoreError> {
        let key_bytes = key.as_bytes();
        let value_bytes = serde_json::to_vec(value)?;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(self.table_def)?;
            table.insert(key_bytes.as_slice(), value_bytes.as_slice())?;
        }
        write_txn.commit()?;

        tracing::trace!(key = %key, "Stored value in table");
        Ok(())
    }

    /// Get a value by UUID key.
    pub(crate) fn get(&self, key: Uuid) -> Result<Option<V>, KeyValueStoreError> {
        let key_bytes = key.as_bytes();

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(self.table_def)?;

        match table.get(key_bytes.as_slice())? {
            Some(value) => {
                let result: V = serde_json::from_slice(value.value())?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    /// Remove a value by UUID key. Returns true if the key existed.
    pub(crate) fn remove(&self, key: Uuid) -> Result<bool, KeyValueStoreError> {
        let key_bytes = key.as_bytes();

        let write_txn = self.db.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(self.table_def)?;
            table.remove(key_bytes.as_slice())?.is_some()
        };
        write_txn.commit()?;

        if removed {
            tracing::trace!(key = %key, "Removed value from table");
        }

        Ok(removed)
    }

    /// Update a value using a closure, performing read-modify-write atomically.
    /// If the key doesn't exist, creates a new entry using the default value.
    pub(crate) fn update<F>(
        &self,
        key: Uuid,
        default: V,
        update_fn: F,
    ) -> Result<(), KeyValueStoreError>
    where
        F: FnOnce(&mut V),
    {
        let key_bytes = key.as_bytes();

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(self.table_def)?;

            // Read existing value or use default
            let mut value: V = match table.get(key_bytes.as_slice())? {
                Some(existing) => {
                    let bytes: &[u8] = existing.value();
                    serde_json::from_slice(bytes)?
                }
                None => default,
            };

            // Apply update
            update_fn(&mut value);

            // Write back
            let value_bytes = serde_json::to_vec(&value)?;
            table.insert(key_bytes.as_slice(), value_bytes.as_slice())?;
        }
        write_txn.commit()?;

        tracing::trace!(key = %key, "Updated value in table");
        Ok(())
    }

    /// Collect up to `limit` keys whose values satisfy `predicate`.
    ///
    /// This is intended for maintenance tasks (e.g., TTL cleanup) and performs
    /// a full table scan until the limit is reached.
    pub(crate) fn collect_keys_matching<F>(
        &self,
        limit: usize,
        mut predicate: F,
    ) -> Result<Vec<Uuid>, KeyValueStoreError>
    where
        F: FnMut(&V) -> bool,
    {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(self.table_def)?;
        let mut keys = Vec::new();

        for entry in table.iter()? {
            let (key, value) = entry?;
            let key_bytes = key.value();
            let value_bytes = value.value();

            let uuid = Uuid::from_slice(key_bytes).map_err(|_| KeyValueStoreError::InvalidUuid)?;
            let decoded: V = serde_json::from_slice(value_bytes)?;

            if predicate(&decoded) {
                keys.push(uuid);
                if keys.len() >= limit {
                    break;
                }
            }
        }

        Ok(keys)
    }
}

/// Key-Value Store Manager
///
/// Provides access to typed tables backed by redb.
/// Each table stores values keyed by UUID, with automatic JSON serialization.
pub(crate) struct KeyValueStoreManager {
    db: Arc<Database>,
}

impl KeyValueStoreManager {
    /// Open or create a key-value store at the given path.
    ///
    /// Creates parent directories if they don't exist.
    pub(crate) fn connect(path: impl AsRef<Path>) -> Result<Self, KeyValueStoreError> {
        let path = path.as_ref();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(path)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Get a typed table handle.
    ///
    /// The table is created if it doesn't exist.
    /// The type parameter V determines the value type stored in the table.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let manager = KeyValueStoreManager::connect("data.redb")?;
    /// let results_table: Table<MyResult> = manager.table("operation_results")?;
    /// results_table.store(uuid, &my_result)?;
    /// ```
    pub(crate) fn table<V: Serialize + DeserializeOwned>(
        &self,
        name: &'static str,
    ) -> Result<Table<V>, KeyValueStoreError> {
        let table_def: TableDef = TableDefinition::new(name);

        // Ensure the table exists
        let write_txn = self.db.begin_write()?;
        {
            let _table = write_txn.open_table(table_def)?;
        }
        write_txn.commit()?;

        Ok(Table::new(Arc::clone(&self.db), table_def))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use serde::Deserialize;
    use tempfile::TempDir;

    use super::*;

    /// Check if a key exists.
    fn exists(table: &Table<TestData>, key: Uuid) -> Result<bool, KeyValueStoreError> {
        let key_bytes = key.as_bytes();

        let read_txn = table.db.begin_read()?;
        let table = read_txn.open_table(table.table_def)?;

        Ok(table.get(key_bytes.as_slice())?.is_some())
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        name: String,
        count: u32,
    }

    #[test]
    fn test_store_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = Uuid::new_v4();
        let data = TestData {
            name: "test".to_string(),
            count: 42,
        };

        table.store(key, &data).unwrap();

        let retrieved = table.get(key).unwrap();
        assert_eq!(retrieved, Some(data));
    }

    #[test]
    fn test_get_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = Uuid::new_v4();
        let result = table.get(key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_remove() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = Uuid::new_v4();
        let data = TestData {
            name: "test".to_string(),
            count: 1,
        };

        table.store(key, &data).unwrap();
        assert!(exists(&table, key).unwrap());

        let removed = table.remove(key).unwrap();
        assert!(removed);
        assert!(!exists(&table, key).unwrap());

        // Removing again should return false
        let removed_again = table.remove(key).unwrap();
        assert!(!removed_again);
    }

    #[test]
    fn test_update() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = Uuid::new_v4();

        // Store initial value
        table
            .store(
                key,
                &TestData {
                    name: "initial".to_string(),
                    count: 1,
                },
            )
            .unwrap();

        // Update
        table
            .update(
                key,
                TestData {
                    name: "default".to_string(),
                    count: 0,
                },
                |data| {
                    data.count += 1;
                },
            )
            .unwrap();

        let retrieved = table.get(key).unwrap().unwrap();
        assert_eq!(retrieved.name, "initial"); // Name unchanged
        assert_eq!(retrieved.count, 2); // Count incremented
    }

    #[test]
    fn test_update_creates_default() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = Uuid::new_v4();

        // Update non-existent key (should create from default)
        table
            .update(
                key,
                TestData {
                    name: "default".to_string(),
                    count: 0,
                },
                |data| {
                    data.count = 42;
                },
            )
            .unwrap();

        let retrieved = table.get(key).unwrap().unwrap();
        assert_eq!(retrieved.name, "default");
        assert_eq!(retrieved.count, 42);
    }

    #[test]
    fn test_multiple_tables() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table1: Table<TestData> = manager.table("table1").unwrap();
        let table2: Table<String> = manager.table("table2").unwrap();

        let key = Uuid::new_v4();

        table1
            .store(
                key,
                &TestData {
                    name: "test".to_string(),
                    count: 1,
                },
            )
            .unwrap();
        table2.store(key, &"hello".to_string()).unwrap();

        // Both tables should have independent data
        assert!(table1.get(key).unwrap().is_some());
        assert_eq!(table2.get(key).unwrap(), Some("hello".to_string()));
    }
}
