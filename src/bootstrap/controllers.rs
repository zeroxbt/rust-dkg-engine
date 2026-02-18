use std::sync::Arc;

use crate::{
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
    services::Services,
};

pub(crate) fn build_controllers(
    config: &Config,
    managers: &Managers,
    services: &Services,
    command_scheduler: &CommandScheduler,
) -> controllers::Controllers {
    controllers::initialize(
        &config.http_api,
        &config.rpc,
        RpcRouterDeps {
            publish_store: PublishStoreRpcControllerDeps {
                store_response_channels: Arc::clone(&services.response_channels.store),
                command_scheduler: command_scheduler.clone(),
            },
            get: GetRpcControllerDeps {
                get_response_channels: Arc::clone(&services.response_channels.get),
                command_scheduler: command_scheduler.clone(),
            },
            publish_finality: PublishFinalityRpcControllerDeps {
                finality_response_channels: Arc::clone(&services.response_channels.finality),
                command_scheduler: command_scheduler.clone(),
            },
            batch_get: BatchGetRpcControllerDeps {
                batch_get_response_channels: Arc::clone(&services.response_channels.batch_get),
                command_scheduler: command_scheduler.clone(),
            },
        },
        HttpApiDeps {
            command_scheduler: command_scheduler.clone(),
            repository_manager: Arc::clone(&managers.repository),
            get_operation_status_service: Arc::clone(&services.get_operation),
            publish_store_operation_status_service: Arc::clone(&services.publish_store_operation),
        },
    )
}
