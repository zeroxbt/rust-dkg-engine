mod commands;
mod config;
mod context;
mod controllers;
mod error;
mod network;
mod services;
mod types;

use std::sync::Arc;

use ::network::NetworkManager;
use blockchain::BlockchainManager;
use commands::command_executor::{CommandExecutionRequest, CommandExecutor};
use config::ManagersConfig;
use context::Context;
use controllers::{
    blockchain_event_controller::BlockchainEventController,
    http_api_controller::http_api_router::{HttpApiConfig, HttpApiRouter},
    rpc_controller::rpc_router::RpcRouter,
};
use dotenvy::dotenv;
use repository::RepositoryManager;
use services::{
    operation_manager::{OperationConfig, OperationManager},
    ual_service::UalService,
};
use tokio::{
    join,
    sync::mpsc::{Receiver, Sender},
};
use validation::ValidationManager;

use crate::{
    config::Config,
    network::NetworkProtocols,
    services::{file_service::FileService, pending_storage_service::PendingStorageService},
};

#[tokio::main]
async fn main() {
    dotenv().ok();
    initialize_logger();
    display_ot_node_ascii_art();
    let config = Arc::new(config::initialize_configuration());

    let (schedule_command_tx, schedule_command_rx) = initialize_channels();

    // Channels for network manager event loop
    let (network_event_tx, network_event_rx) = tokio::sync::mpsc::channel(1024);

    let (network_manager, repository_manager, blockchain_manager, validation_manager) =
        initialize_managers(&config.managers).await;
    let (ual_service, publish_operation_manager, pending_storage_service) = initialize_services(
        &config,
        &blockchain_manager,
        &repository_manager,
        &validation_manager,
        &network_manager,
    );

    let store_session_manager = Arc::new(network::SessionManager::new());
    let get_session_manager = Arc::new(network::SessionManager::new());

    let context = Arc::new(Context::new(
        config.clone(),
        schedule_command_tx,
        Arc::clone(&repository_manager),
        Arc::clone(&network_manager),
        Arc::clone(&blockchain_manager),
        Arc::clone(&validation_manager),
        Arc::clone(&ual_service),
        Arc::clone(&publish_operation_manager),
        Arc::clone(&pending_storage_service),
        Arc::clone(&store_session_manager),
        Arc::clone(&get_session_manager),
    ));

    if config.is_dev_env {
        initialize_dev_environment(&blockchain_manager).await;
    }

    let command_executor = Arc::new(CommandExecutor::new(Arc::clone(&context)).await);

    let (http_api_router, rpc_router) = initialize_controllers(&config.http_api, &context);

    // Initialize blockchain event controller
    let blockchain_event_controller = BlockchainEventController::new(Arc::clone(&context));

    let cloned_command_executor = Arc::clone(&command_executor);

    let schedule_commands_task = tokio::task::spawn(async move {
        cloned_command_executor
            .listen_and_schedule_commands(schedule_command_rx)
            .await
    });

    let execute_commands_task =
        tokio::task::spawn(async move { command_executor.listen_and_execute_commands().await });

    // Spawn network manager event loop task
    let network_event_loop_task = tokio::task::spawn(async move {
        if let Err(error) = network_manager.start_listening().await {
            tracing::error!("Failed to start swarm listener: {}", error);
            return;
        }

        // Run the network manager event loop
        network_manager.run(network_event_tx).await;
    });

    // Spawn RPC router task to handle network events
    let handle_network_events_task = tokio::task::spawn(async move {
        rpc_router
            .listen_and_handle_network_events(network_event_rx)
            .await
    });
    let handle_http_events_task =
        tokio::task::spawn(async move { http_api_router.listen_and_handle_http_requests().await });

    // Spawn blockchain event listener task
    let blockchain_event_listener_task =
        tokio::task::spawn(
            async move { blockchain_event_controller.listen_and_handle_events().await },
        );

    let _ = join!(
        handle_http_events_task,
        network_event_loop_task,
        handle_network_events_task,
        schedule_commands_task,
        execute_commands_task,
        blockchain_event_listener_task,
    );
}

fn initialize_logger() {
    let filter =
        tracing_subscriber::EnvFilter::new("blockchain=trace,network=trace,rust_ot_node=trace");
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn display_ot_node_ascii_art() {
    tracing::info!(" ██████╗ ████████╗███╗   ██╗ ██████╗ ██████╗ ███████╗");
    tracing::info!("██╔═══██╗╚══██╔══╝████╗  ██║██╔═══██╗██╔══██╗██╔════╝");
    tracing::info!("██║   ██║   ██║   ██╔██╗ ██║██║   ██║██║  ██║█████╗");
    tracing::info!("██║   ██║   ██║   ██║╚██╗██║██║   ██║██║  ██║██╔══╝");
    tracing::info!("╚██████╔╝   ██║   ██║ ╚████║╚██████╔╝██████╔╝███████╗");
    tracing::info!(" ╚═════╝    ╚═╝   ╚═╝  ╚═══╝ ╚═════╝ ╚═════╝ ╚══════╝");

    tracing::info!("======================================================");
    tracing::info!(
        "             OriginTrail Node v{}",
        env!("CARGO_PKG_VERSION")
    );
    tracing::info!("======================================================");
    if let Ok(environment) = std::env::var("NODE_ENV") {
        tracing::info!("Node is running in {} environment", environment);
    } else {
        tracing::error!("NODE_ENV environment variable not set!");
    }
}

fn initialize_channels() -> (
    Sender<CommandExecutionRequest>,
    Receiver<CommandExecutionRequest>,
) {
    let (schedule_command_tx, schedule_command_rx) =
        tokio::sync::mpsc::channel::<CommandExecutionRequest>(1000);

    (schedule_command_tx, schedule_command_rx)
}

async fn initialize_managers(
    config: &ManagersConfig,
) -> (
    Arc<NetworkManager<NetworkProtocols>>,
    Arc<RepositoryManager>,
    Arc<BlockchainManager>,
    Arc<ValidationManager>,
) {
    // NetworkManager creates base protocols (kad, identify, ping) and handles bootstraps
    // Application creates its own protocol behaviours
    let app_protocols = NetworkProtocols::new();
    let network_manager = Arc::new(
        NetworkManager::new(&config.network, app_protocols)
            .await
            .expect("Failed to initialize network manager"),
    );

    let repository_manager = Arc::new(RepositoryManager::new(&config.repository).await.unwrap());
    let mut blockchain_manager = BlockchainManager::new(&config.blockchain).await;
    blockchain_manager
        .initialize_identities(&network_manager.peer_id().to_base58())
        .await
        .unwrap();

    let blockchain_manager = Arc::new(blockchain_manager);
    let validation_manager = Arc::new(ValidationManager::new().await);

    (
        network_manager,
        repository_manager,
        blockchain_manager,
        validation_manager,
    )
}

fn initialize_services(
    config: &Config,
    blockchain_manager: &Arc<BlockchainManager>,
    repository_manager: &Arc<RepositoryManager>,
    _validation_manager: &Arc<ValidationManager>,
    _network_manager: &Arc<NetworkManager<NetworkProtocols>>,
) -> (
    Arc<UalService>,
    Arc<OperationManager>,
    Arc<PendingStorageService>,
) {
    let file_service = Arc::new(FileService::new(config.app_data_path.clone()));
    let pending_storage_service = Arc::new(PendingStorageService::new(Arc::clone(&file_service)));
    let ual_service = Arc::new(UalService::new(Arc::clone(blockchain_manager)));

    let publish_operation_manager = Arc::new(OperationManager::new(
        Arc::clone(repository_manager),
        Arc::clone(&file_service),
        OperationConfig {
            operation_name: "publish",
        },
    ));

    (
        ual_service,
        publish_operation_manager,
        pending_storage_service,
    )
}

fn initialize_controllers(
    http_api_config: &HttpApiConfig,
    context: &Arc<Context>,
) -> (HttpApiRouter, RpcRouter) {
    let http_api_router = HttpApiRouter::new(http_api_config, context);
    let rpc_router = RpcRouter::new(Arc::clone(context));

    (http_api_router, rpc_router)
}

async fn initialize_dev_environment(blockchain_manager: &Arc<BlockchainManager>) {
    use alloy::primitives::utils::parse_ether;

    // 50,000 tokens for stake
    let stake_wei: u128 = parse_ether("50000")
        .expect("Failed to parse stake amount")
        .try_into()
        .expect("Stake amount too large");
    // 0.2 tokens for ask
    let ask_wei: u128 = parse_ether("0.2")
        .expect("Failed to parse ask amount")
        .try_into()
        .expect("Ask amount too large");

    for blockchain_id in blockchain_manager.get_blockchain_ids() {
        if let Err(e) = blockchain_manager.set_stake(blockchain_id, stake_wei).await {
            tracing::error!("Failed to set stake for {}: {}", blockchain_id, e);
            panic!("set-stake did not complete successfully: {}", e);
        }

        if let Err(e) = blockchain_manager.set_ask(blockchain_id, ask_wei).await {
            tracing::error!("Failed to set ask for {}: {}", blockchain_id, e);
            panic!("set-ask did not complete successfully: {}", e);
        }
    }
}
