use dkg_key_value_store::{
    KeyValueStoreManager, OperationResultStore, PublishStoreOperationResult,
};

use crate::operations::OperationKind;

pub(crate) struct PublishStoreOperation;

impl OperationKind for PublishStoreOperation {
    const NAME: &'static str = "publish";
    type Result = PublishStoreOperationResult;

    fn result_store(kv_store_manager: &KeyValueStoreManager) -> OperationResultStore<Self::Result> {
        kv_store_manager.publish_store_operation_result_store()
    }
}
