use std::{sync::Arc, time::Duration};

use dkg_blockchain::BlockchainId;
use dkg_network::NetworkEventLoop;
use tokio::{select, signal::unix::SignalKind};
use tokio_util::sync::CancellationToken;

use crate::{
    commands::{executor::CommandExecutor, scheduler::CommandScheduler},
    controllers::{
        http_api_controller::router::HttpApiRouter, rpc_controller::rpc_router::RpcRouter,
    },
    logger, periodic,
    periodic::tasks::{
        cleanup::CleanupConfig, paranet_sync::ParanetSyncConfig, proving::ProvingConfig,
        sync::SyncConfig,
    },
    services::PeerService,
};

const PERIODIC_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);
const COMMAND_EXECUTOR_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);
const NETWORK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct RuntimeDeps {
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) network_manager: Arc<dkg_network::NetworkManager>,
    pub(crate) peer_service: Arc<PeerService>,
    pub(crate) periodic_deps: Arc<periodic::PeriodicDeps>,
    pub(crate) blockchain_ids: Vec<BlockchainId>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    deps: RuntimeDeps,
    command_executor: CommandExecutor,
    network_event_loop: NetworkEventLoop,
    rpc_router: RpcRouter,
    http_router: Option<HttpApiRouter>,
    cleanup_config: CleanupConfig,
    sync_config: SyncConfig,
    paranet_sync_config: ParanetSyncConfig,
    proving_config: ProvingConfig,
) {
    let command_scheduler = deps.command_scheduler.clone();
    let network_manager = Arc::clone(&deps.network_manager);
    // Spawn peer service loop for network observations.
    let peer_event_rx = deps.network_manager.subscribe_peer_events();
    let peer_service = Arc::clone(&deps.peer_service);
    let _peer_registry_task = peer_service.start(peer_event_rx);

    // Create HTTP shutdown channel (oneshot for single signal)
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn command executor task (executor is consumed)
    let mut execute_commands_task = tokio::task::spawn(async move { command_executor.run().await });

    let periodic_shutdown = CancellationToken::new();
    let mut periodic_handle = tokio::task::spawn(periodic::run_all(
        Arc::clone(&deps.periodic_deps),
        deps.blockchain_ids.clone(),
        cleanup_config,
        sync_config,
        paranet_sync_config,
        proving_config,
        periodic_shutdown.clone(),
    ));

    // Spawn network service event loop task with RPC router as the event handler
    let network_event_loop_task = tokio::task::spawn(async move {
        // Run the network service event loop with the RPC router handling events
        // The service is consumed here (runs until action channel closes)
        network_event_loop.run(&rpc_router).await;
    });

    // Spawn HTTP API task if enabled
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
    // 2. Cancel periodic tasks (they finish current iteration and exit)
    // 3. Wait for periodic tasks to exit
    // 4. Signal command scheduler to stop accepting new commands
    // 5. Wait for command executor to drain
    // 6. Signal network loop to stop
    // 7. Wait for network loop to exit
    // 8. Wait for HTTP to finish in-flight requests
    // 9. Flush telemetry

    tracing::info!("Shutting down gracefully...");

    // Step 1: Signal HTTP server to stop accepting new connections
    let _ = http_shutdown_tx.send(());

    // Step 2: Cancel periodic tasks (they check CancellationToken between iterations)
    periodic_shutdown.cancel();

    // Step 3: Wait for periodic tasks to finish current iteration and exit
    tracing::info!("Waiting for periodic tasks to shut down...");
    match tokio::time::timeout(PERIODIC_SHUTDOWN_TIMEOUT, &mut periodic_handle).await {
        Ok(Ok(())) => tracing::info!("Periodic tasks shut down cleanly"),
        Ok(Err(e)) => tracing::error!("Periodic tasks panicked: {:?}", e),
        Err(_) => {
            tracing::warn!(
                timeout_secs = PERIODIC_SHUTDOWN_TIMEOUT.as_secs(),
                "Periodic tasks shutdown timeout, aborting periodic task group"
            );
            periodic_handle.abort();
            let _ = periodic_handle.await;
        }
    }

    // Step 4: Signal command scheduler to stop accepting new commands
    command_scheduler.shutdown();

    // Step 5: Wait for command executor to drain pending commands
    tracing::info!("Waiting for command executor to drain...");
    match tokio::time::timeout(
        COMMAND_EXECUTOR_SHUTDOWN_TIMEOUT,
        &mut execute_commands_task,
    )
    .await
    {
        Ok(Ok(())) => tracing::info!("Command executor shut down cleanly"),
        Ok(Err(e)) => tracing::error!("Command executor task panicked: {:?}", e),
        Err(_) => {
            tracing::warn!(
                timeout_secs = COMMAND_EXECUTOR_SHUTDOWN_TIMEOUT.as_secs(),
                "Command executor shutdown timeout, aborting command executor task"
            );
            execute_commands_task.abort();
            let _ = execute_commands_task.await;
        }
    }

    // Step 6: Signal network manager event loop to stop
    network_manager.shutdown();

    // Step 7: Wait for network manager to exit
    tracing::info!("Waiting for network manager to shut down...");
    match tokio::time::timeout(NETWORK_SHUTDOWN_TIMEOUT, network_event_loop_task).await {
        Ok(Ok(())) => tracing::info!("Network manager shut down cleanly"),
        Ok(Err(e)) => tracing::error!("Network task panicked: {:?}", e),
        Err(_) => tracing::warn!(
            timeout_secs = NETWORK_SHUTDOWN_TIMEOUT.as_secs(),
            "Network manager shutdown timeout"
        ),
    }

    // Step 8: Wait for HTTP server to finish in-flight requests
    tracing::info!("Waiting for HTTP server to shut down...");
    match tokio::time::timeout(HTTP_SHUTDOWN_TIMEOUT, handle_http_events_task).await {
        Ok(Ok(())) => tracing::info!("HTTP server shut down cleanly"),
        Ok(Err(e)) => tracing::error!("HTTP task panicked: {:?}", e),
        Err(_) => tracing::warn!(
            timeout_secs = HTTP_SHUTDOWN_TIMEOUT.as_secs(),
            "HTTP server shutdown timeout"
        ),
    }

    // Step 9: Flush OpenTelemetry traces
    logger::shutdown_telemetry();

    tracing::info!("Shutdown complete");
}
