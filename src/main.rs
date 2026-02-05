mod commands;
mod config;
mod context;
mod controllers;
mod error;
mod logger;
mod managers;
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
use managers::network::KeyManager;
use tokio::{select, signal::unix::SignalKind, sync::oneshot};

use crate::config::AppPaths;

#[tokio::main]
async fn main() {
    // Install rustls crypto provider before any TLS connections
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    // Load configuration first, then initialize logger with config settings
    let config = Arc::new(config::initialize_configuration());
    logger::initialize(&config.logger, &config.telemetry);

    display_ot_node_ascii_art();

    // Derive all filesystem paths from the root data directory
    let paths = AppPaths::from_root(config.app_data_path.clone());

    // Load or generate network identity key (security-critical, handled at app level)
    let network_key = KeyManager::load_or_generate(&paths.network_key)
        .await
        .expect("Failed to load or generate network identity key");

    // Create command scheduler channel
    let (command_scheduler, command_rx) = CommandScheduler::channel();
    let command_scheduler_for_shutdown = command_scheduler.clone();

    // Initialize managers and services
    let (managers, mut network_event_loop) =
        managers::initialize(&config.managers, &paths, network_key).await;

    // Start listening before spawning the network task
    if let Err(error) = network_event_loop.start_listening() {
        tracing::error!("Failed to start swarm listener: {}", error);
        return;
    }

    // Clone refs needed after managers is moved into Context
    let blockchain_manager_for_ids = Arc::clone(&managers.blockchain);

    if config::is_dev_env() {
        initialize_dev_environment(&managers.blockchain).await;
    }

    let services = services::initialize(&managers, config.managers.network.rate_limiter.clone());

    // Clone scheduler for the executor before moving into context
    let command_scheduler_for_executor = command_scheduler.clone();

    let context = Arc::new(Context::new(command_scheduler, managers, services));

    let command_executor = CommandExecutor::new(Arc::clone(&context), command_rx);

    // Schedule default commands (including per-blockchain event listeners)
    let blockchain_ids: Vec<_> = blockchain_manager_for_ids
        .get_blockchain_ids()
        .into_iter()
        .cloned()
        .collect();
    for request in default_command_requests(&blockchain_ids, &config.cleanup) {
        command_scheduler_for_executor.schedule(request).await;
    }

    let controllers = controllers::initialize(&config.http_api, &context);

    // Create HTTP shutdown channel (oneshot for single signal)
    let (http_shutdown_tx, http_shutdown_rx) = oneshot::channel::<()>();

    // Spawn command executor task (executor is consumed, scheduler is borrowed)
    let execute_commands_task = tokio::task::spawn(async move {
        command_executor.run(&command_scheduler_for_executor).await
    });

    // Spawn network service event loop task with RPC router as the event handler
    let rpc_router = controllers.rpc_router;
    let network_event_loop_task = tokio::task::spawn(async move {
        // Run the network service event loop with the RPC router handling events
        // The service is consumed here (runs until action channel closes)
        network_event_loop.run(&rpc_router).await;
    });

    // Spawn HTTP API task if enabled
    let http_router = controllers.http_router;
    let handle_http_events_task = tokio::task::spawn(async move {
        if let Some(router) = http_router {
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

    // Step 2: Signal command scheduler to stop accepting new commands
    // This prevents spawned delay tasks from scheduling new commands
    command_scheduler_for_shutdown.shutdown();

    // Step 3: Drop command scheduler to close the command channel
    // This causes the executor to stop accepting new commands
    drop(context);

    // Step 3: Wait for command executor to drain pending commands
    // Commands still have network access during this phase
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
    tracing::info!("Waiting for command executor to drain...");
    match tokio::time::timeout(SHUTDOWN_TIMEOUT, execute_commands_task).await {
        Ok(Ok(())) => tracing::info!("Command executor shut down cleanly"),
        Ok(Err(e)) => tracing::error!("Command executor task panicked: {:?}", e),
        Err(_) => tracing::warn!(
            "Command executor drain timeout after {:?}",
            SHUTDOWN_TIMEOUT
        ),
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

    // Step 7: Flush OpenTelemetry traces
    logger::shutdown_telemetry();

    tracing::info!("Shutdown complete");
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
    let environment = config::current_env();
    tracing::info!("Node is running in {} environment", environment);
}

async fn initialize_dev_environment(blockchain_manager: &Arc<managers::BlockchainManager>) {
    use alloy::primitives::utils::parse_ether;

    tracing::info!("Initializing dev environment: setting stake and ask...");

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
