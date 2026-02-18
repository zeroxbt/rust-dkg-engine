use dkg_domain::Assertion;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use uuid::Uuid;

use crate::{KeyValueStoreError, Table};

pub(crate) const GET_OPERATION_RESULTS_TABLE_NAME: &str = "get_operation_results";
pub(crate) const PUBLISH_STORE_OPERATION_RESULTS_TABLE_NAME: &str =
    "publish_store_operation_results";

/// Result stored after a successful get operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetOperationResult {
    /// The retrieved assertion data (public and optionally private triples).
    pub assertion: Assertion,
    /// Optional metadata triples if requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<String>>,
}

impl GetOperationResult {
    pub fn new(assertion: Assertion, metadata: Option<Vec<String>>) -> Self {
        Self {
            assertion,
            metadata,
        }
    }
}

/// Signature data stored after publish store operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishStoreSignatureData {
    pub identity_id: String,
    pub v: u8,
    pub r: String,
    pub s: String,
    pub vs: String,
}

impl PublishStoreSignatureData {
    pub fn new(identity_id: String, v: u8, r: String, s: String, vs: String) -> Self {
        Self {
            identity_id,
            v,
            r,
            s,
            vs,
        }
    }
}

/// Result stored after successful publish store operation.
///
/// Contains all signatures collected during the publish operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishStoreOperationResult {
    /// The publisher's own signature over the dataset.
    pub publisher_signature: Option<PublishStoreSignatureData>,
    /// Signatures from network nodes that stored the dataset.
    pub network_signatures: Vec<PublishStoreSignatureData>,
}

impl PublishStoreOperationResult {
    pub fn new(
        publisher_signature: Option<PublishStoreSignatureData>,
        network_signatures: Vec<PublishStoreSignatureData>,
    ) -> Self {
        Self {
            publisher_signature,
            network_signatures,
        }
    }
}

#[derive(Error, Debug)]
pub enum ResultStoreError {
    #[error("Key-value store error: {0}")]
    Store(#[from] KeyValueStoreError),
}

/// Store for operation results (typed, JSON-serialized).
#[derive(Clone)]
pub struct OperationResultStore<R> {
    table: Table<R>,
}

impl<R> OperationResultStore<R>
where
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub(crate) fn from_table(table: Table<R>) -> Self {
        Self { table }
    }

    pub async fn store_result(&self, operation_id: Uuid, result: R) -> Result<(), ResultStoreError>
    where
        R: Send + 'static,
    {
        self.table
            .store(operation_id.as_bytes().to_vec(), result)
            .await?;
        Ok(())
    }

    pub async fn remove_result(&self, operation_id: Uuid) -> Result<bool, ResultStoreError>
    where
        R: Send + 'static,
    {
        Ok(self.table.remove(operation_id.as_bytes().to_vec()).await?)
    }

    pub async fn get_result(&self, operation_id: Uuid) -> Result<Option<R>, ResultStoreError>
    where
        R: Send + 'static,
    {
        Ok(self.table.get(operation_id.as_bytes().to_vec()).await?)
    }

    pub async fn update_result<F>(
        &self,
        operation_id: Uuid,
        default: R,
        update_fn: F,
    ) -> Result<(), ResultStoreError>
    where
        F: FnOnce(&mut R) + Send + 'static,
        R: Send + 'static,
    {
        self.table
            .update(operation_id.as_bytes().to_vec(), default, update_fn)
            .await?;
        Ok(())
    }
}
