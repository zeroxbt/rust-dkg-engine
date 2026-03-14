use std::sync::Arc;

use dkg_network::{NetworkEventLoop, PeerEvent};
use tokio::{select, signal::unix::SignalKind, sync::broadcast, task::JoinError};
use tokio_util::sync::CancellationToken;

use super::{RuntimeConfig, RuntimeDeps, shutdown};
use crate::{
    commands::executor::CommandExecutor,
    controllers::{
        http_api_controller::router::HttpApiRouter, rpc_controller::rpc_router::RpcRouter,
    },
    peer_registry::PeerRegistry,
    tasks::{
        dkg_sync,
        periodic::{self, PeriodicTasksConfig},
    },
};

enum ShutdownTrigger {
    Signal,
    CriticalTaskExited {
        task: &'static str,
        result: Result<(), JoinError>,
    },
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    deps: RuntimeDeps,
    command_executor: CommandExecutor,
    network_event_loop: NetworkEventLoop,
    rpc_router: RpcRouter,
    http_router: Option<HttpApiRouter>,
    runtime_config: RuntimeConfig,
    periodic_tasks_config: PeriodicTasksConfig,
    metrics_enabled: bool,
) {
    let command_scheduler = deps.command_scheduler.clone();
    let network_manager = Arc::clone(&deps.network_manager);
    // Spawn peer registry updater loop for network observations.
    let peer_event_rx = deps.network_manager.subscribe_peer_events();
    let peer_registry_shutdown = CancellationToken::new();
    let peer_registry_task = tokio::task::spawn(run_peer_registry_updater(
        Arc::clone(&deps.peer_registry),
        peer_event_rx,
        peer_registry_shutdown.clone(),
    ));

    // Create HTTP shutdown channel (oneshot for single signal)
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn command executor task (executor is consumed)
    let mut execute_commands_task =
        Some(tokio::task::spawn(
            async move { command_executor.run().await },
        ));

    let periodic_shutdown = CancellationToken::new();
    let dkg_sync_shutdown = CancellationToken::new();
    let mut dkg_sync_handle = Some(tokio::task::spawn(dkg_sync::run(
        deps.periodic_tasks_deps.dkg_sync.clone(),
        deps.blockchain_ids.clone(),
        periodic_tasks_config.dkg_sync.clone(),
        periodic_tasks_config.reorg_buffer_blocks.max(1),
        dkg_sync_shutdown.clone(),
    )));

    let mut periodic_handle = Some(tokio::task::spawn(periodic::run(
        Arc::clone(&deps.periodic_tasks_deps),
        deps.blockchain_ids.clone(),
        periodic_tasks_config,
        metrics_enabled,
        periodic_shutdown.clone(),
    )));

    // Spawn network service event loop task with RPC router as the event handler
    let mut network_event_loop_task = Some(tokio::task::spawn(async move {
        // Run the network service event loop with the RPC router handling events
        // The service is consumed here (runs until action channel closes)
        network_event_loop.run(&rpc_router).await;
    }));

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

    let shutdown_trigger = select! {
        _ = wait_for_shutdown_signal() => ShutdownTrigger::Signal,
        result = execute_commands_task.as_mut().expect("command executor task initialized") => {
            let _ = execute_commands_task.take();
            ShutdownTrigger::CriticalTaskExited { task: "command_executor", result }
        }
        result = dkg_sync_handle.as_mut().expect("dkg sync task initialized") => {
            let _ = dkg_sync_handle.take();
            ShutdownTrigger::CriticalTaskExited { task: "dkg_sync", result }
        }
        result = periodic_handle.as_mut().expect("periodic task runner initialized") => {
            let _ = periodic_handle.take();
            ShutdownTrigger::CriticalTaskExited { task: "periodic_tasks", result }
        }
        result = network_event_loop_task.as_mut().expect("network event loop task initialized") => {
            let _ = network_event_loop_task.take();
            ShutdownTrigger::CriticalTaskExited { task: "network_event_loop", result }
        }
    };

    if let ShutdownTrigger::CriticalTaskExited { task, result } = &shutdown_trigger {
        log_critical_task_exit(task, result);
    }

    shutdown::graceful_shutdown(shutdown::ShutdownContext {
        command_scheduler,
        network_manager,
        graceful_shutdown: runtime_config.graceful_shutdown,
        dkg_sync_shutdown,
        dkg_sync_handle,
        periodic_shutdown,
        periodic_handle,
        execute_commands_task,
        network_event_loop_task,
        peer_registry_shutdown,
        peer_registry_task,
        http_shutdown_tx,
        handle_http_events_task,
    })
    .await;
}

fn log_critical_task_exit(task: &str, result: &Result<(), JoinError>) {
    match result {
        Ok(()) => tracing::error!(
            task,
            "Critical task exited unexpectedly; initiating shutdown"
        ),
        Err(error) if error.is_panic() => {
            tracing::error!(task, error = ?error, "Critical task panicked; initiating shutdown")
        }
        Err(error) if error.is_cancelled() => {
            tracing::warn!(task, error = ?error, "Critical task was cancelled; initiating shutdown")
        }
        Err(error) => {
            tracing::error!(task, error = ?error, "Critical task failed; initiating shutdown")
        }
    }
}

async fn run_peer_registry_updater(
    peer_registry: Arc<PeerRegistry>,
    mut peer_event_rx: broadcast::Receiver<PeerEvent>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                tracing::info!("Peer registry updater shutdown token cancelled, stopping loop");
                break;
            }
            recv = peer_event_rx.recv() => {
                match recv {
                    Ok(event) => peer_registry.apply_peer_event(event),
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "Peer registry updater lagged, events were dropped");
                    }
                }
            }
        }
    }
}

async fn wait_for_shutdown_signal() {
    let sigterm = tokio::signal::unix::signal(SignalKind::terminate());

    match sigterm {
        Ok(mut sigterm) => {
            select! {
                ctrl_c_result = tokio::signal::ctrl_c() => {
                    match ctrl_c_result {
                        Ok(()) => tracing::info!("Received SIGINT, initiating shutdown..."),
                        Err(error) => tracing::warn!(
                            error = %error,
                            "Failed to receive SIGINT; initiating shutdown"
                        ),
                    }
                },
                _ = sigterm.recv() => tracing::info!("Received SIGTERM, initiating shutdown..."),
            }
        }
        Err(error) => {
            tracing::warn!(
                error = %error,
                "Failed to install SIGTERM handler; waiting for SIGINT only"
            );

            match tokio::signal::ctrl_c().await {
                Ok(()) => tracing::info!("Received SIGINT, initiating shutdown..."),
                Err(ctrl_c_error) => tracing::warn!(
                    error = %ctrl_c_error,
                    "Failed to receive SIGINT; initiating shutdown"
                ),
            }
        }
    }
}
