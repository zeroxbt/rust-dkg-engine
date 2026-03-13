use crate::commands::scheduler::CommandScheduler;

#[derive(Clone)]
pub(crate) struct RpcRouterDeps {
    pub(crate) command_scheduler: CommandScheduler,
}
