use std::sync::Arc;

use tokio::sync::mpsc;

use crate::{
    commands::{
        HandleBatchGetRequestDeps, HandleGetRequestDeps, HandlePublishFinalityRequestDeps,
        HandlePublishStoreRequestDeps, SendGetRequestsDeps, SendPublishFinalityRequestDeps,
        SendPublishStoreRequestsDeps,
        executor::{CommandExecutionRequest, CommandExecutor},
        registry::{CommandResolver, CommandResolverDeps},
        scheduler::CommandScheduler,
    },
    managers::Managers,
    services::Services,
};

pub(crate) fn build_command_executor(
    managers: &Managers,
    services: &Services,
    command_scheduler: &CommandScheduler,
    command_rx: mpsc::Receiver<CommandExecutionRequest>,
) -> CommandExecutor {
    let publish_tmp_dataset_store = Arc::new(managers.key_value_store.publish_tmp_dataset_store());

    let command_resolver = CommandResolver::new(CommandResolverDeps {
        send_publish_store_requests: SendPublishStoreRequestsDeps {
            network_manager: Arc::clone(&managers.network),
            peer_service: Arc::clone(&services.peer_service),
            blockchain_manager: Arc::clone(&managers.blockchain),
            publish_store_operation_status_service: Arc::clone(&services.publish_store_operation),
            publish_tmp_dataset_store: Arc::clone(&publish_tmp_dataset_store),
        },
        handle_publish_store_request: HandlePublishStoreRequestDeps {
            network_manager: Arc::clone(&managers.network),
            blockchain_manager: Arc::clone(&managers.blockchain),
            peer_service: Arc::clone(&services.peer_service),
            store_response_channels: Arc::clone(&services.response_channels.store),
            publish_tmp_dataset_store: Arc::clone(&publish_tmp_dataset_store),
        },
        send_publish_finality_request: SendPublishFinalityRequestDeps {
            repository_manager: Arc::clone(&managers.repository),
            network_manager: Arc::clone(&managers.network),
            peer_service: Arc::clone(&services.peer_service),
            blockchain_manager: Arc::clone(&managers.blockchain),
            publish_tmp_dataset_store: Arc::clone(&publish_tmp_dataset_store),
            triple_store_service: Arc::clone(&services.triple_store),
        },
        handle_publish_finality_request: HandlePublishFinalityRequestDeps {
            repository_manager: Arc::clone(&managers.repository),
            network_manager: Arc::clone(&managers.network),
            finality_response_channels: Arc::clone(&services.response_channels.finality),
        },
        send_get_requests: SendGetRequestsDeps {
            get_operation_status_service: Arc::clone(&services.get_operation),
            get_fetch_service: Arc::clone(&services.get_fetch),
        },
        handle_get_request: HandleGetRequestDeps {
            network_manager: Arc::clone(&managers.network),
            triple_store_service: Arc::clone(&services.triple_store),
            peer_service: Arc::clone(&services.peer_service),
            get_response_channels: Arc::clone(&services.response_channels.get),
            blockchain_manager: Arc::clone(&managers.blockchain),
        },
        handle_batch_get_request: HandleBatchGetRequestDeps {
            network_manager: Arc::clone(&managers.network),
            triple_store_service: Arc::clone(&services.triple_store),
            peer_service: Arc::clone(&services.peer_service),
            batch_get_response_channels: Arc::clone(&services.response_channels.batch_get),
        },
    });

    CommandExecutor::new(command_scheduler.clone(), command_resolver, command_rx)
}
