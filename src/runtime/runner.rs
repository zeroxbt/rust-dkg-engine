use std::sync::Arc;

use dkg_network::NetworkEventLoop;
use tokio::{select, signal::unix::SignalKind};
use tokio_util::sync::CancellationToken;

use super::{RuntimeDeps, shutdown};
use crate::{
    commands::executor::CommandExecutor,
    controllers::{
        http_api_controller::router::HttpApiRouter, rpc_controller::rpc_router::RpcRouter,
    },
    periodic_tasks,
    periodic_tasks::tasks::{
        cleanup::CleanupConfig, paranet_sync::ParanetSyncConfig, proving::ProvingConfig,
        sync::SyncConfig,
    },
};

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
    let execute_commands_task = tokio::task::spawn(async move { command_executor.run().await });

    let periodic_shutdown = CancellationToken::new();
    let periodic_handle = tokio::task::spawn(periodic_tasks::run(
        Arc::clone(&deps.periodic_tasks_deps),
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

    shutdown::graceful_shutdown(shutdown::ShutdownContext {
        command_scheduler,
        network_manager,
        periodic_shutdown,
        periodic_handle,
        execute_commands_task,
        network_event_loop_task,
        http_shutdown_tx,
        handle_http_events_task,
    })
    .await;
}
