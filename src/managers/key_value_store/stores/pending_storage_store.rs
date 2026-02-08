use std::time::Duration;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::managers::{
    key_value_store::{KeyValueStoreError, KeyValueStoreManager, Table},
    triple_store::Assertion,
};

/// Table name for pending storage data.
const TABLE_NAME: &str = "pending_storage";

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct PendingStorageData {
    dataset_root: String,
    dataset: Assertion,
    publisher_peer_id: String,
    #[serde(default = "default_stored_at")]
    stored_at: i64,
}

impl PendingStorageData {
    pub(crate) fn new(dataset_root: String, dataset: Assertion, publisher_peer_id: String) -> Self {
        Self {
            dataset_root,
            dataset,
            publisher_peer_id,
            stored_at: Utc::now().timestamp_millis(),
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

    pub(crate) fn stored_at(&self) -> i64 {
        self.stored_at
    }
}

fn default_stored_at() -> i64 {
    0
}

/// Store for pending datasets awaiting finality confirmation.
pub(crate) struct PendingStorageStore {
    table: Table<PendingStorageData>,
}

impl PendingStorageStore {
    pub(crate) fn new(kv_store_manager: &KeyValueStoreManager) -> Result<Self, KeyValueStoreError> {
        let table = kv_store_manager.table(TABLE_NAME)?;
        Ok(Self { table })
    }

    pub(crate) fn store(
        &self,
        operation_id: Uuid,
        data: &PendingStorageData,
    ) -> Result<(), KeyValueStoreError> {
        self.table.store(operation_id.as_bytes(), data)
    }

    pub(crate) fn get(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<PendingStorageData>, KeyValueStoreError> {
        self.table.get(operation_id.as_bytes())
    }

    pub(crate) fn remove(&self, operation_id: Uuid) -> Result<bool, KeyValueStoreError> {
        self.table.remove(operation_id.as_bytes())
    }

    pub(crate) fn remove_expired(
        &self,
        ttl: Duration,
        max_remove: usize,
    ) -> Result<usize, KeyValueStoreError> {
        let ttl_ms = ttl.as_millis() as i64;
        let cutoff = Utc::now().timestamp_millis().saturating_sub(ttl_ms);

        let keys = self
            .table
            .collect_keys_matching(max_remove, |data| data.stored_at() <= cutoff)?;

        let mut removed = 0usize;
        for key in keys {
            if self.table.remove(&key)? {
                removed += 1;
            }
        }

        Ok(removed)
    }
}
