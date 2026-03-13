use std::sync::Arc;

use crate::{
    bootstrap::ApplicationDeps,
    commands::scheduler::CommandScheduler,
    config::Config,
    controllers::{
        http_api_controller::HttpApiDeps,
        rpc_controller::RpcRouterDeps,
        {self},
    },
    managers::Managers,
};

pub(crate) fn build_controllers(
    config: &Config,
    managers: &Managers,
    application: &ApplicationDeps,
    command_scheduler: &CommandScheduler,
) -> controllers::Controllers {
    controllers::initialize(
        &config.controllers,
        RpcRouterDeps {
            command_scheduler: command_scheduler.clone(),
        },
        HttpApiDeps {
            command_scheduler: command_scheduler.clone(),
            operation_repository: managers.repository.operation_repository(),
            finality_status_repository: managers.repository.finality_status_repository(),
            get_operation_tracking: Arc::clone(&application.get_operation_tracking),
            publish_store_operation_tracking: Arc::clone(
                &application.publish_store_operation_tracking,
            ),
        },
    )
}
