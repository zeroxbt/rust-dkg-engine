use std::time::Duration;

use chrono::Utc;
use dkg_domain::Assertion;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{KeyValueStoreError, KeyValueStoreManager, Table};

/// Table name for publish temporary datasets.
const TABLE_NAME: &str = "publish_tmp_dataset";

#[derive(Serialize, Deserialize, Clone)]
pub struct PublishTmpDataset {
    dataset_root: String,
    dataset: Assertion,
    publisher_peer_id: String,
    #[serde(default = "default_stored_at")]
    stored_at: i64,
}

impl PublishTmpDataset {
    pub fn new(dataset_root: String, dataset: Assertion, publisher_peer_id: String) -> Self {
        Self {
            dataset_root,
            dataset,
            publisher_peer_id,
            stored_at: Utc::now().timestamp_millis(),
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

    pub fn stored_at(&self) -> i64 {
        self.stored_at
    }
}

fn default_stored_at() -> i64 {
    0
}

/// Store for publish temporary datasets awaiting finality confirmation.
pub struct PublishTmpDatasetStore {
    table: Table<PublishTmpDataset>,
}

impl PublishTmpDatasetStore {
    pub fn new(kv_store_manager: &KeyValueStoreManager) -> Result<Self, KeyValueStoreError> {
        let table = kv_store_manager.table(TABLE_NAME)?;
        Ok(Self { table })
    }

    pub async fn store(
        &self,
        operation_id: Uuid,
        data: PublishTmpDataset,
    ) -> Result<(), KeyValueStoreError> {
        self.table
            .store(operation_id.as_bytes().to_vec(), data)
            .await
    }

    pub async fn get(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<PublishTmpDataset>, KeyValueStoreError> {
        self.table.get(operation_id.as_bytes().to_vec()).await
    }

    pub async fn remove(&self, operation_id: Uuid) -> Result<bool, KeyValueStoreError> {
        self.table.remove(operation_id.as_bytes().to_vec()).await
    }

    pub async fn remove_expired(
        &self,
        ttl: Duration,
        max_remove: usize,
    ) -> Result<usize, KeyValueStoreError> {
        let ttl_ms = ttl.as_millis() as i64;
        let cutoff = Utc::now().timestamp_millis().saturating_sub(ttl_ms);

        let keys = self
            .table
            .collect_keys_matching(max_remove, move |data| data.stored_at() <= cutoff)
            .await?;

        let mut removed = 0usize;
        for key in keys {
            if self.table.remove(key).await? {
                removed += 1;
            }
        }

        Ok(removed)
    }
}
