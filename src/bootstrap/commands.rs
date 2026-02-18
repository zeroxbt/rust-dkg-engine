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
            publish_store_workflow: Arc::clone(&application.publish_store_workflow),
        },
        handle_publish_store_request: HandlePublishStoreRequestDeps {
            network_manager: Arc::clone(&managers.network),
            handle_publish_store_request_workflow: Arc::clone(
                &application.handle_publish_store_request_workflow,
            ),
            store_response_channels: Arc::clone(&node_state.store_response_channels),
        },
        send_publish_finality_request: SendPublishFinalityRequestDeps {
            publish_finality_workflow: Arc::clone(&application.publish_finality_workflow),
        },
        handle_publish_finality_request: HandlePublishFinalityRequestDeps {
            handle_publish_finality_request_workflow: Arc::clone(
                &application.handle_publish_finality_request_workflow,
            ),
            network_manager: Arc::clone(&managers.network),
            finality_response_channels: Arc::clone(&node_state.finality_response_channels),
        },
        send_get_requests: SendGetRequestsDeps {
            get_operation_workflow: Arc::clone(&application.get_operation_workflow),
        },
        handle_get_request: HandleGetRequestDeps {
            network_manager: Arc::clone(&managers.network),
            handle_get_request_workflow: Arc::clone(&application.handle_get_request_workflow),
            get_response_channels: Arc::clone(&node_state.get_response_channels),
        },
        handle_batch_get_request: HandleBatchGetRequestDeps {
            network_manager: Arc::clone(&managers.network),
            handle_batch_get_request_workflow: Arc::clone(
                &application.handle_batch_get_request_workflow,
            ),
            batch_get_response_channels: Arc::clone(&node_state.batch_get_response_channels),
        },
    });

    CommandExecutor::new(command_scheduler.clone(), command_resolver, command_rx)
}
