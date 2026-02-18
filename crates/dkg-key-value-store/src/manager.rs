use std::{path::Path, sync::Arc};

use redb::{Database, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::Semaphore;

use super::{
    KeyValueStoreError, KeyValueStoreManagerConfig,
    stores::{
        GetOperationResult, OperationResultStore, PeerAddressStore, PublishStoreOperationResult,
        PublishTmpDatasetStore, operation_result_store as operation_result_store_mod,
        peer_address_store as peer_address_store_mod,
        publish_tmp_dataset_store as publish_tmp_dataset_store_mod,
    },
    table::{Table, TableDef},
};

/// Key-Value Store Manager
///
/// Provides access to typed tables backed by redb.
/// Each table stores values with automatic JSON serialization.
pub struct KeyValueStoreManager {
    db: Arc<Database>,
    concurrency_limiter: Arc<Semaphore>,
    peer_address_store: PeerAddressStore,
    publish_tmp_dataset_store: PublishTmpDatasetStore,
    get_operation_result_store: OperationResultStore<GetOperationResult>,
    publish_store_operation_result_store: OperationResultStore<PublishStoreOperationResult>,
}

impl KeyValueStoreManager {
    pub async fn connect(
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

            // Initialize required tables up-front. After connect succeeds,
            // required store handles are guaranteed to be available.
            let write_txn = db.begin_write()?;
            {
                let peer_addresses_def: TableDef =
                    TableDefinition::new(peer_address_store_mod::TABLE_NAME);
                let publish_tmp_def: TableDef =
                    TableDefinition::new(publish_tmp_dataset_store_mod::TABLE_NAME);
                let get_operation_results_def: TableDef = TableDefinition::new(
                    operation_result_store_mod::GET_OPERATION_RESULTS_TABLE_NAME,
                );
                let publish_store_operation_results_def: TableDef = TableDefinition::new(
                    operation_result_store_mod::PUBLISH_STORE_OPERATION_RESULTS_TABLE_NAME,
                );

                let _peer_addresses = write_txn.open_table(peer_addresses_def)?;
                let _publish_tmp = write_txn.open_table(publish_tmp_def)?;
                let _get_operation_results = write_txn.open_table(get_operation_results_def)?;
                let _publish_store_operation_results =
                    write_txn.open_table(publish_store_operation_results_def)?;
            }
            write_txn.commit()?;

            let db = Arc::new(db);
            let concurrency_limiter = Arc::new(Semaphore::new(max_concurrent));
            let peer_address_store = PeerAddressStore::from_table(Table::new(
                Arc::clone(&db),
                TableDefinition::new(peer_address_store_mod::TABLE_NAME),
                Arc::clone(&concurrency_limiter),
            ));
            let publish_tmp_dataset_store = PublishTmpDatasetStore::from_table(Table::new(
                Arc::clone(&db),
                TableDefinition::new(publish_tmp_dataset_store_mod::TABLE_NAME),
                Arc::clone(&concurrency_limiter),
            ));
            let get_operation_result_store = OperationResultStore::from_table(Table::new(
                Arc::clone(&db),
                TableDefinition::new(operation_result_store_mod::GET_OPERATION_RESULTS_TABLE_NAME),
                Arc::clone(&concurrency_limiter),
            ));
            let publish_store_operation_result_store =
                OperationResultStore::from_table(Table::new(
                    Arc::clone(&db),
                    TableDefinition::new(
                        operation_result_store_mod::PUBLISH_STORE_OPERATION_RESULTS_TABLE_NAME,
                    ),
                    Arc::clone(&concurrency_limiter),
                ));

            Ok(Self {
                db,
                concurrency_limiter,
                peer_address_store,
                publish_tmp_dataset_store,
                get_operation_result_store,
                publish_store_operation_result_store,
            })
        })
        .await?
    }

    /// Get a typed table handle.
    ///
    /// The table is created if it doesn't exist.
    /// The type parameter V determines the value type (JSON-serialized).
    /// Keys are raw bytes (`&[u8]`).
    pub fn table<V: Serialize + DeserializeOwned>(
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

    /// Return the initialized handle to the publish temporary dataset store.
    pub fn publish_tmp_dataset_store(&self) -> PublishTmpDatasetStore {
        self.publish_tmp_dataset_store.clone()
    }

    /// Return the initialized handle to the peer address store.
    pub fn peer_address_store(&self) -> PeerAddressStore {
        self.peer_address_store.clone()
    }

    pub fn get_operation_result_store(&self) -> OperationResultStore<GetOperationResult> {
        self.get_operation_result_store.clone()
    }

    pub fn publish_store_operation_result_store(
        &self,
    ) -> OperationResultStore<PublishStoreOperationResult> {
        self.publish_store_operation_result_store.clone()
    }
}
