use crate::commands::scheduler::CommandScheduler;

#[derive(Clone)]
pub(crate) struct PublishStoreRpcControllerDeps {
    pub(crate) command_scheduler: CommandScheduler,
}

#[derive(Clone)]
pub(crate) struct GetRpcControllerDeps {
    pub(crate) command_scheduler: CommandScheduler,
}

#[derive(Clone)]
pub(crate) struct PublishFinalityRpcControllerDeps {
    pub(crate) command_scheduler: CommandScheduler,
}

#[derive(Clone)]
pub(crate) struct BatchGetRpcControllerDeps {
    pub(crate) command_scheduler: CommandScheduler,
}

#[derive(Clone)]
pub(crate) struct RpcRouterDeps {
    pub(crate) publish_store: PublishStoreRpcControllerDeps,
    pub(crate) get: GetRpcControllerDeps,
    pub(crate) publish_finality: PublishFinalityRpcControllerDeps,
    pub(crate) batch_get: BatchGetRpcControllerDeps,
}
