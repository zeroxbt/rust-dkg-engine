use std::sync::Arc;

use tokio::sync::mpsc;

use crate::{
    bootstrap::ApplicationDeps,
    commands::{
        HandleBatchGetRequestDeps, HandleGetRequestDeps, HandlePublishFinalityRequestDeps,
        HandlePublishStoreRequestDeps, SendGetRequestsDeps, SendPublishFinalityRequestDeps,
        SendPublishStoreRequestsDeps,
        executor::{CommandExecutionRequest, CommandExecutor},
        registry::{CommandResolver, CommandResolverDeps},
        scheduler::CommandScheduler,
    },
    managers::Managers,
    runtime_state::RuntimeState,
};

pub(crate) fn build_command_executor(
    managers: &Managers,
    runtime_state: &RuntimeState,
    application: &ApplicationDeps,
    command_scheduler: &CommandScheduler,
    command_rx: mpsc::Receiver<CommandExecutionRequest>,
) -> CommandExecutor {
    let publish_tmp_dataset_store = Arc::new(managers.key_value_store.publish_tmp_dataset_store());
    let finality_status_repository = managers.repository.finality_status_repository();
    let triples_insert_count_repository = managers.repository.triples_insert_count_repository();

    let command_resolver = CommandResolver::new(CommandResolverDeps {
        send_publish_store_requests: SendPublishStoreRequestsDeps {
            network_manager: Arc::clone(&managers.network),
            peer_directory: Arc::clone(&runtime_state.peer_directory),
            blockchain_manager: Arc::clone(&managers.blockchain),
            publish_store_operation_tracking: Arc::clone(
                &application.publish_store_operation_tracking,
            ),
            publish_tmp_dataset_store: Arc::clone(&publish_tmp_dataset_store),
        },
        handle_publish_store_request: HandlePublishStoreRequestDeps {
            network_manager: Arc::clone(&managers.network),
            blockchain_manager: Arc::clone(&managers.blockchain),
            peer_directory: Arc::clone(&runtime_state.peer_directory),
            store_response_channels: Arc::clone(&runtime_state.response_channels.store),
            publish_tmp_dataset_store: Arc::clone(&publish_tmp_dataset_store),
        },
        send_publish_finality_request: SendPublishFinalityRequestDeps {
            finality_status_repository: finality_status_repository.clone(),
            triples_insert_count_repository: triples_insert_count_repository.clone(),
            network_manager: Arc::clone(&managers.network),
            peer_directory: Arc::clone(&runtime_state.peer_directory),
            blockchain_manager: Arc::clone(&managers.blockchain),
            publish_tmp_dataset_store: Arc::clone(&publish_tmp_dataset_store),
            triple_store_assertions: Arc::clone(&application.triple_store_assertions),
        },
        handle_publish_finality_request: HandlePublishFinalityRequestDeps {
            finality_status_repository,
            network_manager: Arc::clone(&managers.network),
            finality_response_channels: Arc::clone(&runtime_state.response_channels.finality),
        },
        send_get_requests: SendGetRequestsDeps {
            get_operation_tracking: Arc::clone(&application.get_operation_tracking),
            get_assertion_use_case: Arc::clone(&application.get_assertion_use_case),
        },
        handle_get_request: HandleGetRequestDeps {
            network_manager: Arc::clone(&managers.network),
            triple_store_assertions: Arc::clone(&application.triple_store_assertions),
            peer_directory: Arc::clone(&runtime_state.peer_directory),
            get_response_channels: Arc::clone(&runtime_state.response_channels.get),
            blockchain_manager: Arc::clone(&managers.blockchain),
        },
        handle_batch_get_request: HandleBatchGetRequestDeps {
            network_manager: Arc::clone(&managers.network),
            triple_store_assertions: Arc::clone(&application.triple_store_assertions),
            peer_directory: Arc::clone(&runtime_state.peer_directory),
            batch_get_response_channels: Arc::clone(&runtime_state.response_channels.batch_get),
        },
    });

    CommandExecutor::new(command_scheduler.clone(), command_resolver, command_rx)
}
