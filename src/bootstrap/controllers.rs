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
    node_state::NodeState,
};

pub(crate) fn build_controllers(
    config: &Config,
    managers: &Managers,
    node_state: &NodeState,
    application: &ApplicationDeps,
    command_scheduler: &CommandScheduler,
) -> controllers::Controllers {
    controllers::initialize(
        &config.http_api,
        &config.rpc,
        RpcRouterDeps {
            publish_store: PublishStoreRpcControllerDeps {
                store_response_channels: Arc::clone(&node_state.store_response_channels),
                command_scheduler: command_scheduler.clone(),
            },
            get: GetRpcControllerDeps {
                get_response_channels: Arc::clone(&node_state.get_response_channels),
                command_scheduler: command_scheduler.clone(),
            },
            publish_finality: PublishFinalityRpcControllerDeps {
                finality_response_channels: Arc::clone(&node_state.finality_response_channels),
                command_scheduler: command_scheduler.clone(),
            },
            batch_get: BatchGetRpcControllerDeps {
                batch_get_response_channels: Arc::clone(&node_state.batch_get_response_channels),
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
