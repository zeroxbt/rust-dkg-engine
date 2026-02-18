use std::sync::Arc;

use crate::{
    application::{
        AssertionValidation, GetAssertionUseCase, OperationTracking, TripleStoreAssertions,
    },
    managers::Managers,
    operations::{GetOperation, PublishStoreOperation},
    runtime_state::RuntimeState,
};

pub(crate) struct ApplicationDeps {
    pub(crate) publish_store_operation_tracking: Arc<OperationTracking<PublishStoreOperation>>,
    pub(crate) get_operation_tracking: Arc<OperationTracking<GetOperation>>,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
    pub(crate) assertion_validation: Arc<AssertionValidation>,
    pub(crate) get_assertion_use_case: Arc<GetAssertionUseCase>,
}

pub(crate) fn build_application(managers: &Managers, runtime_state: &RuntimeState) -> ApplicationDeps {
    let operation_repository = managers.repository.operation_repository();

    let publish_store_operation_tracking = Arc::new(OperationTracking::<PublishStoreOperation>::new(
        operation_repository.clone(),
        &managers.key_value_store,
    ));

    let get_operation_tracking = Arc::new(OperationTracking::<GetOperation>::new(
        operation_repository,
        &managers.key_value_store,
    ));

    let triple_store_assertions = Arc::new(TripleStoreAssertions::new(Arc::clone(
        &managers.triple_store,
    )));

    let assertion_validation = Arc::new(AssertionValidation::new(Arc::clone(&managers.blockchain)));

    let get_assertion_use_case = Arc::new(GetAssertionUseCase::new(
        Arc::clone(&managers.blockchain),
        Arc::clone(&triple_store_assertions),
        Arc::clone(&managers.network),
        Arc::clone(&assertion_validation),
        Arc::clone(&runtime_state.peer_directory),
    ));

    ApplicationDeps {
        publish_store_operation_tracking,
        get_operation_tracking,
        triple_store_assertions,
        assertion_validation,
        get_assertion_use_case,
    }
}
