use std::{sync::Arc, time::Duration};

use tokio::{select, signal::unix::SignalKind};

use crate::{
    commands::command_executor::{CommandExecutor, CommandScheduler},
    context::Context,
    controllers::{
        http_api_controller::http_api_router::HttpApiRouter, rpc_controller::rpc_router::RpcRouter,
    },
    logger,
    managers::network::NetworkEventLoop,
};

pub(crate) async fn run(
    context: Arc<Context>,
    command_executor: CommandExecutor,
    command_scheduler: CommandScheduler,
    network_event_loop: NetworkEventLoop,
    rpc_router: RpcRouter,
    http_router: Option<HttpApiRouter>,
) {
    // Spawn peer service loop for network observations.
    let peer_event_rx = context.network_manager().subscribe_peer_events();
    let peer_service = Arc::clone(context.peer_service());
    let _peer_registry_task = peer_service.start(peer_event_rx);

    // Create HTTP shutdown channel (oneshot for single signal)
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let command_scheduler_for_executor = command_scheduler.clone();
    let command_scheduler_for_shutdown = command_scheduler.clone();

    // Spawn command executor task (executor is consumed, scheduler is borrowed)
    let execute_commands_task =
        tokio::task::spawn(
            async move { command_executor.run(&command_scheduler_for_executor).await },
        );

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
