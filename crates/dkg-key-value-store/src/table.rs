use std::sync::Arc;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::KeyValueStoreError;

/// Table definition type alias for string table names.
pub type TableDef = TableDefinition<'static, &'static [u8], &'static [u8]>;

/// A table handle with byte keys and JSON-serialized values.
///
/// Keys are raw `&[u8]` — callers handle their own type conversions.
/// Values are automatically serialized/deserialized as JSON.
pub struct Table<V> {
    db: Arc<Database>,
    table_def: TableDef,
    concurrency_limiter: Arc<Semaphore>,
    _marker: std::marker::PhantomData<V>,
}

impl<V> Clone for Table<V> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            table_def: self.table_def,
            concurrency_limiter: Arc::clone(&self.concurrency_limiter),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<V: Serialize + DeserializeOwned> Table<V> {
    pub(super) fn new(
        db: Arc<Database>,
        table_def: TableDef,
        concurrency_limiter: Arc<Semaphore>,
    ) -> Self {
        Self {
            db,
            table_def,
            concurrency_limiter,
            _marker: std::marker::PhantomData,
        }
    }

    /// Store a value with a key.
    fn store_blocking(&self, key: &[u8], value: &V) -> Result<(), KeyValueStoreError> {
        let value_bytes = serde_json::to_vec(value)?;

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(self.table_def)?;
            table.insert(key, value_bytes.as_slice())?;
        }
        write_txn.commit()?;

        Ok(())
    }

    async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit, KeyValueStoreError> {
        self.concurrency_limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| KeyValueStoreError::SemaphoreClosed)
    }

    async fn run_blocking<F, T>(&self, f: F) -> Result<T, KeyValueStoreError>
    where
        F: FnOnce(&Table<V>) -> Result<T, KeyValueStoreError> + Send + 'static,
        T: Send + 'static,
        V: Send + Sync + 'static,
    {
        let table = (*self).clone();
        let permit = self.acquire_permit().await?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(&table)
        })
        .await?
    }

    pub async fn store(&self, key: Vec<u8>, value: V) -> Result<(), KeyValueStoreError>
    where
        V: Send + Sync + 'static,
    {
        self.run_blocking(move |table| table.store_blocking(&key, &value))
            .await
    }

    /// Get a value by key.
    fn get_blocking(&self, key: &[u8]) -> Result<Option<V>, KeyValueStoreError> {
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

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<V>, KeyValueStoreError>
    where
        V: Send + Sync + 'static,
    {
        self.run_blocking(move |table| table.get_blocking(&key))
            .await
    }

    /// Remove a value by key. Returns true if the key existed.
    fn remove_blocking(&self, key: &[u8]) -> Result<bool, KeyValueStoreError> {
        let write_txn = self.db.begin_write()?;
        let removed = {
            let mut table = write_txn.open_table(self.table_def)?;
            table.remove(key)?.is_some()
        };
        write_txn.commit()?;

        Ok(removed)
    }

    pub async fn remove(&self, key: Vec<u8>) -> Result<bool, KeyValueStoreError>
    where
        V: Send + Sync + 'static,
    {
        self.run_blocking(move |table| table.remove_blocking(&key))
            .await
    }

    /// Update a value using a closure, performing read-modify-write atomically.
    /// If the key doesn't exist, creates a new entry using the default value.
    fn update_blocking<F>(
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

    pub async fn update<F>(
        &self,
        key: Vec<u8>,
        default: V,
        update_fn: F,
    ) -> Result<(), KeyValueStoreError>
    where
        F: FnOnce(&mut V) + Send + 'static,
        V: Send + Sync + 'static,
    {
        self.run_blocking(move |table| table.update_blocking(&key, default, update_fn))
            .await
    }

    /// Collect up to `limit` keys whose values satisfy `predicate`.
    ///
    /// This is intended for maintenance tasks (e.g., TTL cleanup) and performs
    /// a full table scan until the limit is reached.
    fn collect_keys_matching_blocking<F>(
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

    pub async fn collect_keys_matching<F>(
        &self,
        limit: usize,
        predicate: F,
    ) -> Result<Vec<Vec<u8>>, KeyValueStoreError>
    where
        F: FnMut(&V) -> bool + Send + 'static,
        V: Send + Sync + 'static,
    {
        self.run_blocking(move |table| table.collect_keys_matching_blocking(limit, predicate))
            .await
    }

    /// Get all key-value pairs from the table.
    fn get_all_blocking(&self) -> Result<Vec<(Vec<u8>, V)>, KeyValueStoreError> {
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

    pub async fn get_all(&self) -> Result<Vec<(Vec<u8>, V)>, KeyValueStoreError>
    where
        V: Send + Sync + 'static,
    {
        self.run_blocking(move |table| table.get_all_blocking())
            .await
    }

    /// Remove all entries from the table.
    fn clear_blocking(&self) -> Result<(), KeyValueStoreError> {
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

    pub async fn clear(&self) -> Result<(), KeyValueStoreError>
    where
        V: Send + Sync + 'static,
    {
        self.run_blocking(move |table| table.clear_blocking()).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::sync::atomic::{AtomicU64, Ordering};

    use serde::{Deserialize, Serialize};
    use tempfile::TempDir;

    use super::Table;
    use crate::{KeyValueStoreError, KeyValueStoreManager, KeyValueStoreManagerConfig};

    fn unique_key() -> Vec<u8> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!("key_{}", COUNTER.fetch_add(1, Ordering::Relaxed)).into_bytes()
    }

    fn default_config() -> KeyValueStoreManagerConfig {
        KeyValueStoreManagerConfig {
            max_concurrent_operations: 16,
        }
    }

    /// Check if a key exists.
    async fn exists(table: &Table<TestData>, key: &[u8]) -> Result<bool, KeyValueStoreError> {
        Ok(table.get(key.to_vec()).await?.is_some())
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestData {
        name: String,
        count: u32,
    }

    #[tokio::test]
    async fn test_store_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = unique_key();
        let data = TestData {
            name: "test".to_string(),
            count: 42,
        };

        table.store(key.clone(), data.clone()).await.unwrap();

        let retrieved = table.get(key).await.unwrap();
        assert_eq!(retrieved, Some(data));
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = unique_key();
        let result = table.get(key).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_remove() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = unique_key();
        let data = TestData {
            name: "test".to_string(),
            count: 1,
        };

        table.store(key.clone(), data).await.unwrap();
        assert!(exists(&table, &key).await.unwrap());

        let removed = table.remove(key.clone()).await.unwrap();
        assert!(removed);
        assert!(!exists(&table, &key).await.unwrap());

        // Removing again should return false
        let removed_again = table.remove(key).await.unwrap();
        assert!(!removed_again);
    }

    #[tokio::test]
    async fn test_update() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = unique_key();

        // Store initial value
        table
            .store(
                key.clone(),
                TestData {
                    name: "initial".to_string(),
                    count: 1,
                },
            )
            .await
            .unwrap();

        // Update
        table
            .update(
                key.clone(),
                TestData {
                    name: "default".to_string(),
                    count: 0,
                },
                |data| {
                    data.count += 1;
                },
            )
            .await
            .unwrap();

        let retrieved = table.get(key).await.unwrap().unwrap();
        assert_eq!(retrieved.name, "initial"); // Name unchanged
        assert_eq!(retrieved.count, 2); // Count incremented
    }

    #[tokio::test]
    async fn test_update_creates_default() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();

        let table: Table<TestData> = manager.table("test_table").unwrap();

        let key = unique_key();

        // Update non-existent key (should create from default)
        table
            .update(
                key.clone(),
                TestData {
                    name: "default".to_string(),
                    count: 0,
                },
                |data| {
                    data.count = 42;
                },
            )
            .await
            .unwrap();

        let retrieved = table.get(key).await.unwrap().unwrap();
        assert_eq!(retrieved.name, "default");
        assert_eq!(retrieved.count, 42);
    }

    #[tokio::test]
    async fn test_multiple_tables() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();

        let table1: Table<TestData> = manager.table("table1").unwrap();
        let table2: Table<String> = manager.table("table2").unwrap();

        let key = unique_key();

        table1
            .store(
                key.clone(),
                TestData {
                    name: "test".to_string(),
                    count: 1,
                },
            )
            .await
            .unwrap();
        table2
            .store(key.clone(), "hello".to_string())
            .await
            .unwrap();

        // Both tables should have independent data
        assert!(table1.get(key.clone()).await.unwrap().is_some());
        assert_eq!(table2.get(key).await.unwrap(), Some("hello".to_string()));
    }

    #[tokio::test]
    async fn test_get_all() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();

        let table: Table<u32> = manager.table("test_get_all").unwrap();

        table.store(b"a".to_vec(), 1).await.unwrap();
        table.store(b"b".to_vec(), 2).await.unwrap();
        table.store(b"c".to_vec(), 3).await.unwrap();

        let all = table.get_all().await.unwrap();
        assert_eq!(all.len(), 3);

        let map: std::collections::HashMap<Vec<u8>, u32> = all.into_iter().collect();
        assert_eq!(map.get(b"a".as_slice()), Some(&1));
        assert_eq!(map.get(b"b".as_slice()), Some(&2));
        assert_eq!(map.get(b"c".as_slice()), Some(&3));
    }

    #[tokio::test]
    async fn test_clear() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.redb");
        let manager = KeyValueStoreManager::connect(&db_path, &default_config())
            .await
            .unwrap();

        let table: Table<u32> = manager.table("test_clear").unwrap();

        table.store(b"a".to_vec(), 1).await.unwrap();
        table.store(b"b".to_vec(), 2).await.unwrap();

        table.clear().await.unwrap();

        let all = table.get_all().await.unwrap();
        assert!(all.is_empty());
    }
}
