use std::sync::Arc;

use dkg_network::{BatchGetAck, FinalityAck, GetAck, NetworkManager, StoreAck};

use crate::{
    application::{
        GetOperationWorkflow, HandleBatchGetRequestWorkflow, HandleGetRequestWorkflow,
        HandlePublishFinalityRequestWorkflow, HandlePublishStoreRequestWorkflow,
        PublishFinalityWorkflow, PublishStoreWorkflow,
    },
    node_state::ResponseChannels,
};

#[derive(Clone)]
pub(crate) struct SendPublishStoreRequestsDeps {
    pub(crate) publish_store_workflow: Arc<PublishStoreWorkflow>,
}

#[derive(Clone)]
pub(crate) struct HandlePublishStoreRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) handle_publish_store_request_workflow: Arc<HandlePublishStoreRequestWorkflow>,
    pub(crate) store_response_channels: Arc<ResponseChannels<StoreAck>>,
}

#[derive(Clone)]
pub(crate) struct SendPublishFinalityRequestDeps {
    pub(crate) publish_finality_workflow: Arc<PublishFinalityWorkflow>,
}

#[derive(Clone)]
pub(crate) struct HandlePublishFinalityRequestDeps {
    pub(crate) handle_publish_finality_request_workflow: Arc<HandlePublishFinalityRequestWorkflow>,
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
    pub(crate) handle_get_request_workflow: Arc<HandleGetRequestWorkflow>,
    pub(crate) get_response_channels: Arc<ResponseChannels<GetAck>>,
}

#[derive(Clone)]
pub(crate) struct HandleBatchGetRequestDeps {
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) handle_batch_get_request_workflow: Arc<HandleBatchGetRequestWorkflow>,
    pub(crate) batch_get_response_channels: Arc<ResponseChannels<BatchGetAck>>,
}
