use std::sync::Arc;

use crate::{
    bootstrap::ApplicationDeps,
    commands::scheduler::CommandScheduler,
    config::Config,
    controllers::{
        self,
        http_api_controller::HttpApiDeps,
        rpc_controller::{
            BatchGetRpcControllerDeps, GetRpcControllerDeps, PublishFinalityRpcControllerDeps,
            PublishStoreRpcControllerDeps, RpcRouterDeps,
        },
    },
    managers::Managers,
    runtime_state::RuntimeState,
};

pub(crate) fn build_controllers(
    config: &Config,
    managers: &Managers,
    runtime_state: &RuntimeState,
    application: &ApplicationDeps,
    command_scheduler: &CommandScheduler,
) -> controllers::Controllers {
    controllers::initialize(
        &config.http_api,
        &config.rpc,
        RpcRouterDeps {
            publish_store: PublishStoreRpcControllerDeps {
                store_response_channels: Arc::clone(&runtime_state.response_channels.store),
                command_scheduler: command_scheduler.clone(),
            },
            get: GetRpcControllerDeps {
                get_response_channels: Arc::clone(&runtime_state.response_channels.get),
                command_scheduler: command_scheduler.clone(),
            },
            publish_finality: PublishFinalityRpcControllerDeps {
                finality_response_channels: Arc::clone(&runtime_state.response_channels.finality),
                command_scheduler: command_scheduler.clone(),
            },
            batch_get: BatchGetRpcControllerDeps {
                batch_get_response_channels: Arc::clone(&runtime_state.response_channels.batch_get),
                command_scheduler: command_scheduler.clone(),
            },
        },
        HttpApiDeps {
            command_scheduler: command_scheduler.clone(),
            operation_repository: managers.repository.operation_repository(),
            finality_status_repository: managers.repository.finality_status_repository(),
            get_operation_tracking: Arc::clone(&application.get_operation_tracking),
            publish_store_operation_tracking: Arc::clone(&application.publish_store_operation_tracking),
        },
    )
}
