mod commands;
mod config;
mod context;
mod controllers;
mod error;
mod managers;
mod observability;
mod operations;
mod services;
mod types;
mod utils;

use std::{sync::Arc, time::Duration};

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
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::{select, signal::unix::SignalKind, sync::oneshot};

use crate::{
    config::{AppPaths, ManagersConfig},
    managers::{
        blockchain::BlockchainManager,
        network::{KeyManager, NetworkManager},
        repository::RepositoryManager,
        triple_store::TripleStoreManager,
    },
    operations::{BatchGetOperation, GetOperation, PublishOperation},
    services::{
        GetValidationService, PeerDiscoveryTracker, ResponseChannels, TripleStoreService,
        operation::OperationService as GenericOperationService,
        pending_storage_service::PendingStorageService,
    },
};

#[tokio::main]
async fn main() {
    // Install rustls crypto provider before any TLS connections
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    dotenv().ok();
    initialize_logger();
    display_ot_node_ascii_art();
    let config = Arc::new(config::initialize_configuration());

    // Initialize Prometheus metrics exporter if enabled
    if config.observability.metrics.enabled {
        let metrics_port = config.observability.metrics.port;
        PrometheusBuilder::new()
            .with_http_listener(([0, 0, 0, 0], metrics_port))
            .install()
            .expect("Failed to install Prometheus metrics exporter");
        tracing::info!("Metrics endpoint enabled on port {}", metrics_port);
    }

    // Derive all filesystem paths from the root data directory
    let paths = AppPaths::from_root(config.app_data_path.clone());

    // Load or generate network identity key (security-critical, handled at app level)
    let network_key = KeyManager::load_or_generate(&paths.network_key)
        .await
        .expect("Failed to load or generate network identity key");

    // Create command scheduler channel
    let (command_scheduler, command_rx) = CommandScheduler::channel();

    let (network_manager, repository_manager, blockchain_manager, triple_store_manager) =
        initialize_managers(&config.managers, &paths, network_key).await;
    let store_response_channels = Arc::new(ResponseChannels::new());
    let get_response_channels = Arc::new(ResponseChannels::new());
    let finality_response_channels = Arc::new(ResponseChannels::new());
    let batch_get_response_channels = Arc::new(ResponseChannels::new());
    let get_validation_service =
        Arc::new(GetValidationService::new(Arc::clone(&blockchain_manager)));
    let triple_store_service = Arc::new(TripleStoreService::new(Arc::clone(&triple_store_manager)));

    // Initialize operation services (need result_store and request_tracker)
    let (
        publish_operation_service,
        get_operation_service,
        batch_get_operation_service,
        pending_storage_service,
    ) = initialize_services(&paths, &repository_manager, &network_manager);

    let peer_discovery_tracker = Arc::new(PeerDiscoveryTracker::new());

    let context = Arc::new(Context::new(
        config.clone(),
        command_scheduler,
        Arc::clone(&repository_manager),
        Arc::clone(&network_manager),
        Arc::clone(&blockchain_manager),
        Arc::clone(&triple_store_service),
        Arc::clone(&get_validation_service),
        Arc::clone(&pending_storage_service),
        Arc::clone(&peer_discovery_tracker),
        Arc::clone(&store_response_channels),
        Arc::clone(&get_response_channels),
        Arc::clone(&finality_response_channels),
        Arc::clone(&batch_get_response_channels),
        Arc::clone(&get_operation_service),
        Arc::clone(&publish_operation_service),
        Arc::clone(&batch_get_operation_service),
    ));

    #[cfg(feature = "dev-tools")]
    initialize_dev_environment(&blockchain_manager).await;

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

    // Create HTTP shutdown channel (oneshot for single signal)
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel::<()>();

    // Spawn command executor task
    let execute_commands_task =
        tokio::task::spawn(async move { command_executor.run().await });

    // Spawn network manager event loop task with RPC router as the event handler
    let network_event_loop_task = tokio::task::spawn(async move {
        if let Err(error) = network_manager.start_listening().await {
            tracing::error!("Failed to start swarm listener: {}", error);
            return;
        }

        // Run the network manager event loop with the RPC router handling events
        network_manager.run(&rpc_router).await;
    });

    // Spawn HTTP API task if enabled
    let handle_http_events_task = tokio::task::spawn(async move {
        if let Some(router) = http_api_router {
            router
                .listen_and_handle_http_requests(http_shutdown_rx)
                .await;
        } else {
            // HTTP API disabled - wait for shutdown signal
            let _ = http_shutdown_rx.await;
        }
    });

    // Wait for shutdown signal (SIGINT or SIGTERM)
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("Failed to install SIGTERM handler");

    select! {
        _ = ctrl_c => tracing::info!("Received SIGINT, initiating shutdown..."),
        _ = sigterm.recv() => tracing::info!("Received SIGTERM, initiating shutdown..."),
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // ORDERED SHUTDOWN SEQUENCE
    // ═══════════════════════════════════════════════════════════════════════════
    //
    // 1. Stop HTTP server (stop accepting new requests)
    // 2. Drop command_scheduler (close command channel)
    // 3. Wait for executor to drain (commands still have network access)
    // 4. Drop context (closes network action channel)
    // 5. Wait for network loop to exit
    // 6. Wait for HTTP to finish in-flight requests

    tracing::info!("Shutting down gracefully...");

    // Step 1: Signal HTTP server to stop accepting new connections
    let _ = http_shutdown_tx.send(());

    // Step 2: Drop command scheduler to close the command channel
    // This causes the executor to stop accepting new commands
    drop(context);

    // Step 3: Wait for command executor to drain pending commands
    // Commands still have network access during this phase
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
    tracing::info!("Waiting for command executor to drain...");
    match tokio::time::timeout(SHUTDOWN_TIMEOUT, execute_commands_task).await {
        Ok(Ok(())) => tracing::info!("Command executor shut down cleanly"),
        Ok(Err(e)) => tracing::error!("Command executor task panicked: {:?}", e),
        Err(_) => tracing::warn!("Command executor drain timeout after {:?}", SHUTDOWN_TIMEOUT),
    }

    // Step 4 & 5: Network manager shuts down when its action channel closes
    // (which happened when we dropped context above)
    tracing::info!("Waiting for network manager to shut down...");
    match tokio::time::timeout(Duration::from_secs(5), network_event_loop_task).await {
        Ok(Ok(())) => tracing::info!("Network manager shut down cleanly"),
        Ok(Err(e)) => tracing::error!("Network task panicked: {:?}", e),
        Err(_) => tracing::warn!("Network manager shutdown timeout"),
    }

    // Step 6: Wait for HTTP server to finish in-flight requests
    tracing::info!("Waiting for HTTP server to shut down...");
    match tokio::time::timeout(Duration::from_secs(5), handle_http_events_task).await {
        Ok(Ok(())) => tracing::info!("HTTP server shut down cleanly"),
        Ok(Err(e)) => tracing::error!("HTTP task panicked: {:?}", e),
        Err(_) => tracing::warn!("HTTP server shutdown timeout"),
    }

    tracing::info!("Shutdown complete");
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
    Arc<NetworkManager>,
    Arc<RepositoryManager>,
    Arc<BlockchainManager>,
    Arc<TripleStoreManager>,
) {
    // NetworkManager creates base protocols (kad, identify) and app protocols (store, get, etc.)
    let network_manager = Arc::new(
        NetworkManager::connect(&config.network, network_key)
            .await
            .expect("Failed to initialize network manager"),
    );

    let repository_manager = Arc::new(
        RepositoryManager::connect(&config.repository)
            .await
            .expect("Failed to initialize repository manager"),
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
    network_manager: &Arc<NetworkManager>,
) -> (
    Arc<GenericOperationService<PublishOperation>>,
    Arc<GenericOperationService<GetOperation>>,
    Arc<GenericOperationService<BatchGetOperation>>,
    Arc<PendingStorageService>,
) {
    // Create shared key-value store manager using centralized path
    let kv_store_manager = crate::managers::key_value_store::KeyValueStoreManager::connect(
        paths.key_value_store.clone(),
    )
    .expect("Failed to connect to key-value store manager");

    let pending_storage_service = Arc::new(
        PendingStorageService::new(&kv_store_manager)
            .expect("Failed to create pending storage service"),
    );

    let publish_operation_service = Arc::new(
        GenericOperationService::<PublishOperation>::new(
            Arc::clone(repository_manager),
            Arc::clone(network_manager),
            &kv_store_manager,
        )
        .expect("Failed to create publish operation service"),
    );
    let get_operation_service = Arc::new(
        GenericOperationService::<GetOperation>::new(
            Arc::clone(repository_manager),
            Arc::clone(network_manager),
            &kv_store_manager,
        )
        .expect("Failed to create get operation service"),
    );
    let batch_get_operation_service = Arc::new(
        GenericOperationService::<BatchGetOperation>::new(
            Arc::clone(repository_manager),
            Arc::clone(network_manager),
            &kv_store_manager,
        )
        .expect("Failed to create batch get operation service"),
    );

    (
        publish_operation_service,
        get_operation_service,
        batch_get_operation_service,
        pending_storage_service,
    )
}

fn initialize_controllers(
    http_api_config: &HttpApiConfig,
    context: &Arc<Context>,
) -> (Option<HttpApiRouter>, RpcRouter) {
    let http_api_router = if http_api_config.enabled {
        tracing::info!("HTTP API enabled on port {}", http_api_config.port);
        Some(HttpApiRouter::new(http_api_config, context))
    } else {
        tracing::info!("HTTP API disabled");
        None
    };
    let rpc_router = RpcRouter::new(Arc::clone(context));

    (http_api_router, rpc_router)
}

#[cfg(feature = "dev-tools")]
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
