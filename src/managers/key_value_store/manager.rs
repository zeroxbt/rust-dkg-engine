use std::{path::Path, sync::Arc};

use redb::{Database, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::Semaphore;

use super::{
    stores::{OperationResultStore, PeerAddressStore, PublishTmpDatasetStore, ResultStoreError},
    KeyValueStoreError, KeyValueStoreManagerConfig,
};
use super::table::{Table, TableDef};

/// Key-Value Store Manager
///
/// Provides access to typed tables backed by redb.
/// Each table stores values with automatic JSON serialization.
pub(crate) struct KeyValueStoreManager {
    db: Arc<Database>,
    concurrency_limiter: Arc<Semaphore>,
}

impl KeyValueStoreManager {
    pub(crate) async fn connect(
        path: impl AsRef<Path>,
        config: &KeyValueStoreManagerConfig,
    ) -> Result<Self, KeyValueStoreError> {
        let path = path.as_ref().to_path_buf();
        let config = config.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let db = Database::create(&path)?;
            let max_concurrent = config.max_concurrent_operations.max(1);
            if max_concurrent != config.max_concurrent_operations {
                tracing::warn!(
                    configured = config.max_concurrent_operations,
                    effective = max_concurrent,
                    "Key-value store max_concurrent_operations too low; clamped"
                );
            }
            tracing::info!(
                max_concurrent = max_concurrent,
                "Key-value store concurrency limiter initialized"
            );
            Ok(Self {
                db: Arc::new(db),
                concurrency_limiter: Arc::new(Semaphore::new(max_concurrent)),
            })
        })
        .await?
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

        Ok(Table::new(
            Arc::clone(&self.db),
            table_def,
            Arc::clone(&self.concurrency_limiter),
        ))
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
