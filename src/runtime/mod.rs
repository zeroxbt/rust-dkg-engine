use std::{sync::Arc, time::Duration};

use tokio::{select, signal::unix::SignalKind};
use tokio_util::sync::CancellationToken;

use crate::{
    commands::executor::CommandExecutor,
    context::Context,
    controllers::{
        http_api_controller::router::HttpApiRouter, rpc_controller::rpc_router::RpcRouter,
    },
    logger,
    managers::network::NetworkEventLoop,
    periodic,
    periodic::tasks::cleanup::CleanupConfig,
};

pub(crate) async fn run(
    context: Arc<Context>,
    command_executor: CommandExecutor,
    network_event_loop: NetworkEventLoop,
    rpc_router: RpcRouter,
    http_router: Option<HttpApiRouter>,
    cleanup_config: CleanupConfig,
) {
    let command_scheduler = context.command_scheduler().clone();
    let network_manager = Arc::clone(context.network_manager());
    // Spawn peer service loop for network observations.
    let peer_event_rx = context.network_manager().subscribe_peer_events();
    let peer_service = Arc::clone(context.peer_service());
    let _peer_registry_task = peer_service.start(peer_event_rx);

    // Create HTTP shutdown channel (oneshot for single signal)
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn command executor task (executor is consumed)
    let execute_commands_task = tokio::task::spawn(async move { command_executor.run().await });

    let periodic_shutdown = CancellationToken::new();
    let periodic_handle = tokio::task::spawn(periodic::run_all(
        Arc::clone(&context),
        cleanup_config,
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
    // 5. Drop local runtime context reference
    // 6. Wait for command executor to drain
    // 7. Signal network loop to stop
    // 8. Wait for network loop to exit
    // 9. Wait for HTTP to finish in-flight requests
    // 10. Flush telemetry

    tracing::info!("Shutting down gracefully...");

    // Step 1: Signal HTTP server to stop accepting new connections
    let _ = http_shutdown_tx.send(());

    // Step 2: Cancel periodic tasks (they check CancellationToken between iterations)
    periodic_shutdown.cancel();

    // Step 3: Wait for periodic tasks to finish current iteration and exit
    tracing::info!("Waiting for periodic tasks to shut down...");
    match periodic_handle.await {
        Ok(()) => tracing::info!("Periodic tasks shut down cleanly"),
        Err(e) => tracing::error!("Periodic tasks panicked: {:?}", e),
    }

    // Step 4: Signal command scheduler to stop accepting new commands
    command_scheduler.shutdown();

    // Step 5: Drop local runtime context reference
    drop(context);

    // Step 6: Wait for command executor to drain pending commands
    tracing::info!("Waiting for command executor to drain...");
    match execute_commands_task.await {
        Ok(()) => tracing::info!("Command executor shut down cleanly"),
        Err(e) => tracing::error!("Command executor task panicked: {:?}", e),
    }

    // Step 7: Signal network manager event loop to stop
    network_manager.shutdown();

    // Step 8: Wait for network manager to exit
    tracing::info!("Waiting for network manager to shut down...");
    match tokio::time::timeout(Duration::from_secs(5), network_event_loop_task).await {
        Ok(Ok(())) => tracing::info!("Network manager shut down cleanly"),
        Ok(Err(e)) => tracing::error!("Network task panicked: {:?}", e),
        Err(_) => tracing::warn!("Network manager shutdown timeout"),
    }

    // Step 9: Wait for HTTP server to finish in-flight requests
    tracing::info!("Waiting for HTTP server to shut down...");
    match tokio::time::timeout(Duration::from_secs(5), handle_http_events_task).await {
        Ok(Ok(())) => tracing::info!("HTTP server shut down cleanly"),
        Ok(Err(e)) => tracing::error!("HTTP task panicked: {:?}", e),
        Err(_) => tracing::warn!("HTTP server shutdown timeout"),
    }

    // Step 10: Flush OpenTelemetry traces
    logger::shutdown_telemetry();

    tracing::info!("Shutdown complete");
}
