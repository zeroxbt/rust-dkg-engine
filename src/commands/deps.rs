use std::sync::Arc;

use dkg_network::{BatchGetAck, FinalityAck, GetAck, NetworkManager, StoreAck};

use crate::{
    application::{
        ExecutePublishStoreWorkflow, GetOperationWorkflow, ProcessPublishFinalityEventWorkflow,
        ServeBatchGetWorkflow, ServeGetWorkflow, ServePublishFinalityWorkflow,
        ServePublishStoreWorkflow,
    },
    node_state::ResponseChannels,
};

#[derive(Clone)]
pub(crate) struct SendPublishStoreRequestsDeps {
    pub(crate) execute_publish_store_workflow: Arc<ExecutePublishStoreWorkflow>,
}

#[derive(Clone)]
pub(crate) struct HandlePublishStoreRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) serve_publish_store_workflow: Arc<ServePublishStoreWorkflow>,
    pub(crate) store_response_channels: Arc<ResponseChannels<StoreAck>>,
}

#[derive(Clone)]
pub(crate) struct SendPublishFinalityRequestDeps {
    pub(crate) process_publish_finality_event_workflow: Arc<ProcessPublishFinalityEventWorkflow>,
}

#[derive(Clone)]
pub(crate) struct HandlePublishFinalityRequestDeps {
    pub(crate) serve_publish_finality_workflow: Arc<ServePublishFinalityWorkflow>,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) finality_response_channels: Arc<ResponseChannels<FinalityAck>>,
}

#[derive(Clone)]
pub(crate) struct SendGetRequestsDeps {
    pub(crate) get_operation_workflow: Arc<GetOperationWorkflow>,
}

#[derive(Clone)]
pub(crate) struct HandleGetRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) serve_get_workflow: Arc<ServeGetWorkflow>,
    pub(crate) get_response_channels: Arc<ResponseChannels<GetAck>>,
}

#[derive(Clone)]
pub(crate) struct HandleBatchGetRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) serve_batch_get_workflow: Arc<ServeBatchGetWorkflow>,
    pub(crate) batch_get_response_channels: Arc<ResponseChannels<BatchGetAck>>,
}
