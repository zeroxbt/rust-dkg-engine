use dkg_key_value_store::{GetOperationResult, KeyValueStoreManager, OperationResultStore};

use crate::operations::OperationKind;

pub(crate) struct GetOperation;

impl OperationKind for GetOperation {
    const NAME: &'static str = "get";
    type Result = GetOperationResult;

    fn result_store(kv_store_manager: &KeyValueStoreManager) -> OperationResultStore<Self::Result> {
        kv_store_manager.get_operation_result_store()
    }
}
