use std::sync::Arc;

use dkg_network::{BatchGetAck, FinalityAck, GetAck, StoreAck};

use crate::{commands::scheduler::CommandScheduler, state::ResponseChannels};

#[derive(Clone)]
pub(crate) struct PublishStoreRpcControllerDeps {
    pub(crate) store_response_channels: Arc<ResponseChannels<StoreAck>>,
    pub(crate) command_scheduler: CommandScheduler,
}

#[derive(Clone)]
pub(crate) struct GetRpcControllerDeps {
    pub(crate) get_response_channels: Arc<ResponseChannels<GetAck>>,
    pub(crate) command_scheduler: CommandScheduler,
}

#[derive(Clone)]
pub(crate) struct PublishFinalityRpcControllerDeps {
    pub(crate) finality_response_channels: Arc<ResponseChannels<FinalityAck>>,
    pub(crate) command_scheduler: CommandScheduler,
}

#[derive(Clone)]
pub(crate) struct BatchGetRpcControllerDeps {
    pub(crate) batch_get_response_channels: Arc<ResponseChannels<BatchGetAck>>,
    pub(crate) command_scheduler: CommandScheduler,
}

#[derive(Clone)]
pub(crate) struct RpcRouterDeps {
    pub(crate) publish_store: PublishStoreRpcControllerDeps,
    pub(crate) get: GetRpcControllerDeps,
    pub(crate) publish_finality: PublishFinalityRpcControllerDeps,
    pub(crate) batch_get: BatchGetRpcControllerDeps,
}
