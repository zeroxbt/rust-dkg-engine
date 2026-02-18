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
    node_state::NodeState,
};

pub(crate) fn build_command_executor(
    managers: &Managers,
    node_state: &NodeState,
    application: &ApplicationDeps,
    command_scheduler: &CommandScheduler,
    command_rx: mpsc::Receiver<CommandExecutionRequest>,
) -> CommandExecutor {
    let command_resolver = CommandResolver::new(CommandResolverDeps {
        send_publish_store_requests: SendPublishStoreRequestsDeps {
            execute_publish_store_workflow: Arc::clone(&application.execute_publish_store_workflow),
        },
        handle_publish_store_request: HandlePublishStoreRequestDeps {
            network_manager: Arc::clone(&managers.network),
            serve_publish_store_workflow: Arc::clone(&application.serve_publish_store_workflow),
            store_response_channels: Arc::clone(&node_state.store_response_channels),
        },
        send_publish_finality_request: SendPublishFinalityRequestDeps {
            process_publish_finality_event_workflow: Arc::clone(
                &application.process_publish_finality_event_workflow,
            ),
        },
        handle_publish_finality_request: HandlePublishFinalityRequestDeps {
            serve_publish_finality_workflow: Arc::clone(
                &application.serve_publish_finality_workflow,
            ),
            network_manager: Arc::clone(&managers.network),
            finality_response_channels: Arc::clone(&node_state.finality_response_channels),
        },
        send_get_requests: SendGetRequestsDeps {
            get_operation_workflow: Arc::clone(&application.get_operation_workflow),
        },
        handle_get_request: HandleGetRequestDeps {
            network_manager: Arc::clone(&managers.network),
            serve_get_workflow: Arc::clone(&application.serve_get_workflow),
            get_response_channels: Arc::clone(&node_state.get_response_channels),
        },
        handle_batch_get_request: HandleBatchGetRequestDeps {
            network_manager: Arc::clone(&managers.network),
            serve_batch_get_workflow: Arc::clone(&application.serve_batch_get_workflow),
            batch_get_response_channels: Arc::clone(&node_state.batch_get_response_channels),
        },
    });

    CommandExecutor::new(command_scheduler.clone(), command_resolver, command_rx)
}
