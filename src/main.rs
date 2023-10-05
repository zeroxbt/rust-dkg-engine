mod commands;
mod config;
mod context;
mod controllers;

use blockchain::BlockchainManager;
use commands::command::AbstractCommand;
use commands::command_executor::CommandExecutor;
use config::ManagersConfig;
use context::Context;
use controllers::blockchain_event_controller::BlockchainEventController;
use controllers::http_api_controller::http_api_router::HttpApiConfig;
use controllers::http_api_controller::http_api_router::HttpApiRouter;
use controllers::rpc_controller::rpc_router::RpcRouter;
use dotenvy::dotenv;
use network::action::NetworkAction;
use network::NetworkEvent;
use network::NetworkManager;
use repository::RepositoryManager;
use std::process::Command;
use std::sync::Arc;
use tokio::join;
use tokio::sync::mpsc::{Receiver, Sender};
use validation::ValidationManager;

#[tokio::main]
async fn main() {
    dotenv().ok();
    initialize_logger();
    display_ot_node_ascii_art();
    let config = Arc::new(config::initialize_configuration());

    let (
        network_action_tx,
        network_action_rx,
        network_event_tx,
        network_event_rx,
        schedule_command_tx,
        schedule_command_rx,
    ) = initialize_channels();

    let (network_manager, repository_manager, blockchain_manager, validation_manager) =
        initialize_managers(&config.managers).await;

    let context = Arc::new(Context::new(
        config.clone(),
        network_action_tx.clone(),
        schedule_command_tx,
        Arc::clone(&repository_manager),
        Arc::clone(&network_manager),
        Arc::clone(&blockchain_manager),
        Arc::clone(&validation_manager),
    ));

    if config.is_dev_env {
        tokio::task::spawn(async move {
            initialize_dev_environment(&blockchain_manager).await;
        });
    }

    let command_executor = Arc::new(CommandExecutor::new(Arc::clone(&context)).await);

    let (http_api_router, rpc_router, blockchain_event_controller) =
        initialize_controllers(&config.http_api, &context);

    let cloned_command_executor = Arc::clone(&command_executor);

    let schedule_commands_task = tokio::task::spawn(async move {
        cloned_command_executor
            .listen_and_schedule_commands(schedule_command_rx)
            .await
    });

    blockchain_event_controller
        .retrieve_and_handle_unprocessed_events()
        .await;

    let execute_commands_task =
        tokio::task::spawn(async move { command_executor.listen_and_execute_commands().await });

    let handle_blockchain_events_task =
        tokio::task::spawn(
            async move { blockchain_event_controller.listen_and_handle_events().await },
        );

    let handle_network_events_task = tokio::task::spawn(async move {
        rpc_router
            .listen_and_handle_network_events(network_event_rx)
            .await
    });
    let handle_http_events_task =
        tokio::task::spawn(async move { http_api_router.listen_and_handle_http_requests().await });

    let handle_swarm_events_task = tokio::task::spawn(async move {
        network_manager
            .listen_and_handle_swarm_events(network_action_rx, network_event_tx)
            .await;
    });

    let _ = join!(
        handle_http_events_task,
        handle_network_events_task,
        handle_blockchain_events_task,
        schedule_commands_task,
        execute_commands_task,
        handle_swarm_events_task
    );
}

fn initialize_logger() {
    let filter =
        tracing_subscriber::EnvFilter::new("off,blockchain=trace,network=trace,rust_ot_node=trace");
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
    Sender<NetworkAction>,
    Receiver<NetworkAction>,
    Sender<NetworkEvent>,
    Receiver<NetworkEvent>,
    Sender<Box<dyn AbstractCommand>>,
    Receiver<Box<dyn AbstractCommand>>,
) {
    let (network_action_tx, network_action_rx) = tokio::sync::mpsc::channel::<NetworkAction>(1000);
    let (network_event_tx, network_event_rx) = tokio::sync::mpsc::channel::<NetworkEvent>(1000);
    let (schedule_command_tx, schedule_command_rx) =
        tokio::sync::mpsc::channel::<Box<dyn AbstractCommand>>(1000);

    (
        network_action_tx,
        network_action_rx,
        network_event_tx,
        network_event_rx,
        schedule_command_tx,
        schedule_command_rx,
    )
}

async fn initialize_managers(
    config: &ManagersConfig,
) -> (
    Arc<NetworkManager>,
    Arc<RepositoryManager>,
    Arc<BlockchainManager>,
    Arc<ValidationManager>,
) {
    let network_manager = Arc::new(NetworkManager::new(&config.network).await);
    let repository_manager = Arc::new(RepositoryManager::new(&config.repository).await.unwrap());
    let mut blockchain_manager = BlockchainManager::new(&config.blockchain).await;
    blockchain_manager
        .initialize_identities(&network_manager.get_peer_id().to_base58())
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

fn initialize_controllers(
    http_api_config: &HttpApiConfig,
    context: &Arc<Context>,
) -> (HttpApiRouter, RpcRouter, BlockchainEventController) {
    let http_api_router = HttpApiRouter::new(http_api_config, context);
    let rpc_router = RpcRouter::new(Arc::clone(context));
    let blockchain_event_controller = BlockchainEventController::new(Arc::clone(context));

    (http_api_router, rpc_router, blockchain_event_controller)
}

async fn initialize_dev_environment(blockchain_manager: &Arc<BlockchainManager>) {
    for blockchain in blockchain_manager.get_blockchain_names() {
        let config = blockchain_manager.get_blockchain_config(blockchain);
        let stake_command = format!(
                "cargo run -p scripts -- set-stake --rpcEndpoint={} --stake={} --operationalWalletPrivateKey={} --managementWalletPrivateKey={} --hubContractAddress={}",
                config.rpc_endpoints()[0],
                50_000,
                config.evm_operational_wallet_private_key(),
                config.evm_management_wallet_private_key().unwrap(),
                config.hub_contract_address()
            );

        let ask_command = format!(
                "cargo run -p scripts -- set-ask --rpcEndpoint={} --ask={} --privateKey={} --hubContractAddress={}",
                config.rpc_endpoints()[0],
                0.2,
                config.evm_operational_wallet_private_key(),
                config.hub_contract_address()
            );

        let status = Command::new("sh")
            .arg("-c")
            .arg(stake_command)
            .status()
            .expect("Failed to run set-stake command");

        assert!(
            status.success(),
            "set-stake command did not complete successfully."
        );

        let status = Command::new("sh")
            .arg("-c")
            .arg(ask_command)
            .status()
            .expect("Failed to run set-ask command");

        assert!(
            status.success(),
            "set-ask command did not complete successfully."
        );
    }
}
