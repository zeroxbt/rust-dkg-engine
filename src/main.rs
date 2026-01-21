mod commands;
mod config;
mod context;
mod controllers;
mod error;
mod operations;
mod services;
mod utils;

use std::sync::Arc;

use ::network::{KeyManager, NetworkManager};
use blockchain::BlockchainManager;
use commands::{
    command_executor::{CommandExecutor, CommandScheduler},
    command_registry::default_command_requests,
};
use context::Context;
use controllers::{
    http_api_controller::http_api_router::{HttpApiConfig, HttpApiRouter},
    rpc_controller::rpc_router::RpcRouter,
};
use dotenvy::dotenv;
use libp2p::identity::Keypair;
use repository::RepositoryManager;
use tokio::select;
use triple_store::TripleStoreManager;

use crate::{
    config::{AppPaths, ManagersConfig},
    controllers::rpc_controller::NetworkProtocols,
    operations::{GetOperation, PublishOperation},
    services::{
        GetValidationService, RequestTracker, ResponseChannels, TripleStoreService,
        operation::OperationService as GenericOperationService,
        pending_storage_service::PendingStorageService,
    },
};

#[tokio::main]
async fn main() {
    dotenv().ok();
    initialize_logger();
    display_ot_node_ascii_art();
    let config = Arc::new(config::initialize_configuration());

    // Derive all filesystem paths from the root data directory
    let paths = AppPaths::from_root(config.app_data_path.clone());

    // Load or generate network identity key (security-critical, handled at app level)
    let network_key = KeyManager::load_or_generate(&paths.network_key)
        .await
        .expect("Failed to load or generate network identity key");

    // Create command scheduler channel
    let (command_scheduler, command_rx) = CommandScheduler::channel();

    // Channels for network manager event loop
    let (network_event_tx, network_event_rx) = tokio::sync::mpsc::channel(1024);

    let (network_manager, repository_manager, blockchain_manager, triple_store_manager) =
        initialize_managers(&config.managers, &paths, network_key).await;
    let store_response_channels = Arc::new(ResponseChannels::new());
    let get_response_channels = Arc::new(ResponseChannels::new());
    let finality_response_channels = Arc::new(ResponseChannels::new());
    let get_validation_service =
        Arc::new(GetValidationService::new(Arc::clone(&blockchain_manager)));
    let triple_store_service = Arc::new(TripleStoreService::new(Arc::clone(&triple_store_manager)));

    // Initialize operation services (need result_store and request_tracker)
    let (publish_operation_service, get_operation_service, pending_storage_service) =
        initialize_services(&paths, &repository_manager);

    let context = Arc::new(Context::new(
        config.clone(),
        command_scheduler,
        Arc::clone(&repository_manager),
        Arc::clone(&network_manager),
        Arc::clone(&blockchain_manager),
        Arc::clone(&triple_store_service),
        Arc::clone(&get_validation_service),
        Arc::clone(&pending_storage_service),
        Arc::clone(&store_response_channels),
        Arc::clone(&get_response_channels),
        Arc::clone(&finality_response_channels),
        Arc::clone(&get_operation_service),
        Arc::clone(&publish_operation_service),
    ));

    if config.is_dev_env {
        initialize_dev_environment(&blockchain_manager).await;
    }

    let command_executor = Arc::new(CommandExecutor::new(Arc::clone(&context), command_rx));

    // Schedule default commands (including per-blockchain event listeners)
    let blockchain_ids: Vec<_> = blockchain_manager
        .get_blockchain_ids()
        .into_iter()
        .cloned()
        .collect();
    command_executor
        .schedule_commands(default_command_requests(&blockchain_ids))
        .await;

    let (http_api_router, rpc_router) = initialize_controllers(&config.http_api, &context);

    let execute_commands_task = tokio::task::spawn(async move { command_executor.run().await });

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
    let handle_rpc_events_task = tokio::task::spawn(async move {
        rpc_router
            .listen_and_handle_network_events(network_event_rx)
            .await
    });
    let handle_http_events_task =
        tokio::task::spawn(async move { http_api_router.listen_and_handle_http_requests().await });

    select! {
        result = handle_http_events_task => {
            tracing::error!("HTTP task exited unexpectedly: {:?}", result);
        }
        result = network_event_loop_task => {
            tracing::error!("Network task exited unexpectedly: {:?}", result);
        }
        result = handle_rpc_events_task => {
            tracing::error!("RPC task exited unexpectedly: {:?}", result);
        }
        result = execute_commands_task => {
            tracing::error!("Command executor exited unexpectedly: {:?}", result);
        }
    }

    tracing::error!("Critical task failed, shutting down");
    std::process::exit(1);
}

fn initialize_logger() {
    let filter = tracing_subscriber::EnvFilter::new(
        "blockchain=trace,network=trace,rust_ot_node=trace,triple_store=trace,repository=trace",
    );
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

async fn initialize_managers(
    config: &ManagersConfig,
    paths: &AppPaths,
    network_key: Keypair,
) -> (
    Arc<NetworkManager<NetworkProtocols>>,
    Arc<RepositoryManager>,
    Arc<BlockchainManager>,
    Arc<TripleStoreManager>,
) {
    // NetworkManager creates base protocols (kad, identify, ping) and handles bootstraps
    // Application creates its own protocol behaviours
    let app_protocols = NetworkProtocols::new();
    let network_manager = Arc::new(
        NetworkManager::connect(&config.network, network_key, app_protocols)
            .await
            .expect("Failed to initialize network manager"),
    );

    let repository_manager = Arc::new(
        RepositoryManager::connect(&config.repository)
            .await
            .unwrap(),
    );
    let mut blockchain_manager = BlockchainManager::connect(&config.blockchain)
        .await
        .expect("Failed to initialize blockchain manager");
    blockchain_manager
        .initialize_identities(&network_manager.peer_id().to_base58())
        .await
        .expect("Failed to initialize blockchain identities");

    let blockchain_manager = Arc::new(blockchain_manager);

    // Use centralized triple store path, merging with existing config
    let mut triple_store_config = config.triple_store.clone();
    triple_store_config.data_path = Some(paths.triple_store.clone());

    let triple_store_manager = Arc::new(
        TripleStoreManager::connect(&triple_store_config)
            .await
            .expect("Failed to initialize triple store manager"),
    );

    (
        network_manager,
        repository_manager,
        blockchain_manager,
        triple_store_manager,
    )
}

fn initialize_services(
    paths: &AppPaths,
    repository_manager: &Arc<RepositoryManager>,
) -> (
    Arc<GenericOperationService<PublishOperation>>,
    Arc<GenericOperationService<GetOperation>>,
    Arc<PendingStorageService>,
) {
    // Create shared key-value store manager using centralized path
    let kv_store_manager =
        key_value_store::KeyValueStoreManager::connect(paths.key_value_store.clone())
            .expect("Failed to connect to key-value store manager");

    let pending_storage_service = Arc::new(
        PendingStorageService::new(&kv_store_manager)
            .expect("Failed to create pending storage service"),
    );
    let request_tracker = Arc::new(RequestTracker::new());

    let publish_operation_service = Arc::new(
        GenericOperationService::<PublishOperation>::new(
            Arc::clone(repository_manager),
            &kv_store_manager,
            Arc::clone(&request_tracker),
        )
        .expect("Failed to create publish operation service"),
    );
    let get_operation_service = Arc::new(
        GenericOperationService::<GetOperation>::new(
            Arc::clone(repository_manager),
            &kv_store_manager,
            Arc::clone(&request_tracker),
        )
        .expect("Failed to create get operation service"),
    );

    (
        publish_operation_service,
        get_operation_service,
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
