mod get;
mod publish;

pub(crate) mod protocols;

pub(crate) use get::GetOperationResult;
pub(crate) use publish::{PublishStoreOperationResult, PublishStoreSignatureData};
use serde::{Serialize, de::DeserializeOwned};

pub(crate) trait OperationKind {
    const NAME: &'static str;
    type Result: Serialize + DeserializeOwned + Send + Sync + 'static;
}

pub(crate) use get::GetOperation;
pub(crate) use publish::PublishStoreOperation;
