mod error;
mod stores;

use std::{path::Path, sync::Arc};

pub(crate) use error::KeyValueStoreError;
pub(crate) use stores::{
    OperationResultStore, PeerAddressStore, PublishTmpDataset, PublishTmpDatasetStore,
    ResultStoreError,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};

/// Table definition type alias for string table names.
type TableDef = TableDefinition<'static, &'static [u8], &'static [u8]>;

/// A table handle with byte keys and JSON-serialized values.
///
/// Keys are raw `&[u8]` — callers handle their own type conversions.
/// Values are automatically serialized/deserialized as JSON.
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

    /// Store a value with a key.
    pub(crate) fn store(&self, key: &[u8], value: &V) -> Result<(), KeyValueStoreError> {
        let value_bytes = serde_json::to_vec(value)?;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(self.table_def)?;
            table.insert(key, value_bytes.as_slice())?;
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Get a value by key.
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<V>, KeyValueStoreError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(self.table_def)?;

        match table.get(key)? {
            Some(value) => {
                let result: V = serde_json::from_slice(value.value())?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    /// Remove a value by key. Returns true if the key existed.
    pub(crate) fn remove(&self, key: &[u8]) -> Result<bool, KeyValueStoreError> {
        let write_txn = self.db.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(self.table_def)?;
            table.remove(key)?.is_some()
        };
        write_txn.commit()?;

        Ok(removed)
    }

    /// Update a value using a closure, performing read-modify-write atomically.
    /// If the key doesn't exist, creates a new entry using the default value.
    pub(crate) fn update<F>(
        &self,
        key: &[u8],
        default: V,
        update_fn: F,
    ) -> Result<(), KeyValueStoreError>
    where
        F: FnOnce(&mut V),
    {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(self.table_def)?;

            // Read existing value or use default
            let mut value: V = match table.get(key)? {
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
            table.insert(key, value_bytes.as_slice())?;
        }
        write_txn.commit()?;

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
    ) -> Result<Vec<Vec<u8>>, KeyValueStoreError>
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
            let decoded: V = serde_json::from_slice(value.value())?;

            if predicate(&decoded) {
                keys.push(key.value().to_vec());
                if keys.len() >= limit {
                    break;
                }
            }
        }

        Ok(keys)
    }

    /// Get all key-value pairs from the table.
    pub(crate) fn get_all(&self) -> Result<Vec<(Vec<u8>, V)>, KeyValueStoreError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(self.table_def)?;
        let mut results = Vec::new();

        for entry in table.iter()? {
            let (key, value) = entry?;
            let decoded: V = serde_json::from_slice(value.value())?;
            results.push((key.value().to_vec(), decoded));
        }

        Ok(results)
    }

    /// Remove all entries from the table.
    pub(crate) fn clear(&self) -> Result<(), KeyValueStoreError> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(self.table_def)?;
            // Collect keys first to avoid borrow issues
            let keys: Vec<Vec<u8>> = table
                .iter()?
                .filter_map(|entry| entry.ok().map(|(k, _)| k.value().to_vec()))
                .collect();
            for key in keys {
                table.remove(key.as_slice())?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }
}

/// Key-Value Store Manager
///
/// Provides access to typed tables backed by redb.
/// Each table stores values with automatic JSON serialization.
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
    /// The type parameter V determines the value type (JSON-serialized).
    /// Keys are raw bytes (`&[u8]`).
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

    pub(crate) fn publish_tmp_dataset_store(
        &self,
    ) -> Result<PublishTmpDatasetStore, KeyValueStoreError> {
        PublishTmpDatasetStore::new(self)
    }

    pub(crate) fn peer_address_store(&self) -> Result<PeerAddressStore, KeyValueStoreError> {
        PeerAddressStore::new(self)
    }

    pub(crate) fn operation_result_store<R>(
        &self,
    ) -> Result<OperationResultStore<R>, ResultStoreError>
    where
        R: Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        OperationResultStore::new(self)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::atomic::{AtomicU64, Ordering};

    use serde::Deserialize;
    use tempfile::TempDir;

    use super::*;

    fn unique_key() -> Vec<u8> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("key_{}", COUNTER.fetch_add(1, Ordering::Relaxed)).into_bytes()
    }

    /// Check if a key exists.
    fn exists(table: &Table<TestData>, key: &[u8]) -> Result<bool, KeyValueStoreError> {
        let read_txn = table.db.begin_read()?;
        let table = read_txn.open_table(table.table_def)?;
        Ok(table.get(key)?.is_some())
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

        let key = unique_key();
        let data = TestData {
            name: "test".to_string(),
            count: 42,
        };

        table.store(&key, &data).unwrap();

        let retrieved = table.get(&key).unwrap();
        assert_eq!(retrieved, Some(data));
    }

    #[test]
    fn test_get_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = unique_key();
        let result = table.get(&key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_remove() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = unique_key();
        let data = TestData {
            name: "test".to_string(),
            count: 1,
        };

        table.store(&key, &data).unwrap();
        assert!(exists(&table, &key).unwrap());

        let removed = table.remove(&key).unwrap();
        assert!(removed);
        assert!(!exists(&table, &key).unwrap());

        // Removing again should return false
        let removed_again = table.remove(&key).unwrap();
        assert!(!removed_again);
    }

    #[test]
    fn test_update() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = unique_key();

        // Store initial value
        table
            .store(
                &key,
                &TestData {
                    name: "initial".to_string(),
                    count: 1,
                },
            )
            .unwrap();

        // Update
        table
            .update(
                &key,
                TestData {
                    name: "default".to_string(),
                    count: 0,
                },
                |data| {
                    data.count += 1;
                },
            )
            .unwrap();

        let retrieved = table.get(&key).unwrap().unwrap();
        assert_eq!(retrieved.name, "initial"); // Name unchanged
        assert_eq!(retrieved.count, 2); // Count incremented
    }

    #[test]
    fn test_update_creates_default() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = unique_key();

        // Update non-existent key (should create from default)
        table
            .update(
                &key,
                TestData {
                    name: "default".to_string(),
                    count: 0,
                },
                |data| {
                    data.count = 42;
                },
            )
            .unwrap();

        let retrieved = table.get(&key).unwrap().unwrap();
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

        let key = unique_key();

        table1
            .store(
                &key,
                &TestData {
                    name: "test".to_string(),
                    count: 1,
                },
            )
            .unwrap();
        table2.store(&key, &"hello".to_string()).unwrap();

        // Both tables should have independent data
        assert!(table1.get(&key).unwrap().is_some());
        assert_eq!(table2.get(&key).unwrap(), Some("hello".to_string()));
    }

    #[test]
    fn test_get_all() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<u32> = manager.table("test_get_all").unwrap();

        table.store(b"a", &1).unwrap();
        table.store(b"b", &2).unwrap();
        table.store(b"c", &3).unwrap();

        let all = table.get_all().unwrap();
        assert_eq!(all.len(), 3);

        let map: std::collections::HashMap<Vec<u8>, u32> = all.into_iter().collect();
        assert_eq!(map.get(b"a".as_slice()), Some(&1));
        assert_eq!(map.get(b"b".as_slice()), Some(&2));
        assert_eq!(map.get(b"c".as_slice()), Some(&3));
    }

    #[test]
    fn test_clear() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path).unwrap();

        let table: Table<u32> = manager.table("test_clear").unwrap();

        table.store(b"a", &1).unwrap();
        table.store(b"b", &2).unwrap();

        table.clear().unwrap();

        let all = table.get_all().unwrap();
        assert!(all.is_empty());
    }
}
