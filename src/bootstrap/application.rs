use std::sync::Arc;

use crate::{
    application::{
        AssertionRetrieval, AssertionValidation, ExecutePublishStoreWorkflow, GetAssertionUseCase,
        GetOperationWorkflow, OperationTracking, ProcessPublishFinalityEventWorkflow,
        ServeBatchGetWorkflow, ServeGetWorkflow, ServePublishFinalityWorkflow,
        ServePublishStoreWorkflow, ShardPeerSelection, TripleStoreAssertions,
    },
    managers::Managers,
    node_state::NodeState,
    operations::{GetOperation, PublishStoreOperation},
};

pub(crate) struct ApplicationDeps {
    pub(crate) publish_store_operation_tracking: Arc<OperationTracking<PublishStoreOperation>>,
    pub(crate) get_operation_tracking: Arc<OperationTracking<GetOperation>>,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
    pub(crate) assertion_validation: Arc<AssertionValidation>,
    pub(crate) assertion_retrieval: Arc<AssertionRetrieval>,
    pub(crate) shard_peer_selection: Arc<ShardPeerSelection>,
    pub(crate) get_assertion_use_case: Arc<GetAssertionUseCase>,
    pub(crate) get_operation_workflow: Arc<GetOperationWorkflow>,
    pub(crate) serve_get_workflow: Arc<ServeGetWorkflow>,
    pub(crate) serve_batch_get_workflow: Arc<ServeBatchGetWorkflow>,
    pub(crate) serve_publish_store_workflow: Arc<ServePublishStoreWorkflow>,
    pub(crate) serve_publish_finality_workflow: Arc<ServePublishFinalityWorkflow>,
    pub(crate) execute_publish_store_workflow: Arc<ExecutePublishStoreWorkflow>,
    pub(crate) process_publish_finality_event_workflow: Arc<ProcessPublishFinalityEventWorkflow>,
}

pub(crate) fn build_application(managers: &Managers, node_state: &NodeState) -> ApplicationDeps {
    let operation_repository = managers.repository.operation_repository();
    let finality_status_repository = managers.repository.finality_status_repository();
    let triples_insert_count_repository = managers.repository.triples_insert_count_repository();
    let publish_tmp_dataset_store = Arc::new(managers.key_value_store.publish_tmp_dataset_store());

    let publish_store_operation_tracking =
        Arc::new(OperationTracking::<PublishStoreOperation>::new(
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

    let assertion_retrieval = Arc::new(AssertionRetrieval::new(
        Arc::clone(&managers.blockchain),
        Arc::clone(&triple_store_assertions),
        Arc::clone(&managers.network),
        Arc::clone(&assertion_validation),
    ));

    let shard_peer_selection = Arc::new(ShardPeerSelection::new(
        Arc::clone(&managers.network),
        Arc::clone(&node_state.peer_registry),
    ));

    let get_assertion_use_case = Arc::new(GetAssertionUseCase::new(
        Arc::clone(&assertion_retrieval),
        Arc::clone(&managers.blockchain),
        Arc::clone(&shard_peer_selection),
    ));

    let get_operation_workflow = Arc::new(GetOperationWorkflow::new(
        Arc::clone(&get_assertion_use_case),
        Arc::clone(&get_operation_tracking),
    ));

    let serve_get_workflow = Arc::new(ServeGetWorkflow::new(
        Arc::clone(&triple_store_assertions),
        Arc::clone(&node_state.peer_registry),
        Arc::clone(&managers.blockchain),
    ));

    let execute_publish_store_workflow = Arc::new(ExecutePublishStoreWorkflow::new(
        Arc::clone(&managers.network),
        Arc::clone(&node_state.peer_registry),
        Arc::clone(&managers.blockchain),
        Arc::clone(&publish_store_operation_tracking),
        Arc::clone(&publish_tmp_dataset_store),
    ));

    let process_publish_finality_event_workflow = Arc::new(ProcessPublishFinalityEventWorkflow::new(
        finality_status_repository.clone(),
        triples_insert_count_repository,
        Arc::clone(&managers.network),
        Arc::clone(&node_state.peer_registry),
        Arc::clone(&managers.blockchain),
        Arc::clone(&publish_tmp_dataset_store),
        Arc::clone(&triple_store_assertions),
    ));

    let serve_batch_get_workflow = Arc::new(ServeBatchGetWorkflow::new(
        Arc::clone(&triple_store_assertions),
        Arc::clone(&node_state.peer_registry),
    ));

    let serve_publish_store_workflow = Arc::new(ServePublishStoreWorkflow::new(
        Arc::clone(&managers.blockchain),
        Arc::clone(&node_state.peer_registry),
        Arc::clone(&publish_tmp_dataset_store),
    ));

    let serve_publish_finality_workflow = Arc::new(ServePublishFinalityWorkflow::new(
        finality_status_repository,
    ));

    ApplicationDeps {
        publish_store_operation_tracking,
        get_operation_tracking,
        triple_store_assertions,
        assertion_validation,
        assertion_retrieval,
        shard_peer_selection,
        get_assertion_use_case,
        get_operation_workflow,
        serve_get_workflow,
        serve_batch_get_workflow,
        serve_publish_store_workflow,
        serve_publish_finality_workflow,
        execute_publish_store_workflow,
        process_publish_finality_event_workflow,
    }
}
