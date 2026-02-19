mod get;
mod publish;

use dkg_key_value_store::KeyValueStoreManager;
pub(crate) use dkg_key_value_store::{
    GetOperationResult, OperationResultStore, PublishStoreOperationResult,
};
use serde::{Serialize, de::DeserializeOwned};

pub(crate) trait OperationKind {
    const NAME: &'static str;
    type Result: Serialize + DeserializeOwned + Send + Sync + 'static;
    fn result_store(kv_store_manager: &KeyValueStoreManager) -> OperationResultStore<Self::Result>;
}

pub(crate) use get::GetOperation;
pub(crate) use publish::PublishStoreOperation;
