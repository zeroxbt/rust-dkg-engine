use std::{sync::Arc, time::Duration};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::GracefulShutdownConfig;
use crate::commands::scheduler::CommandScheduler;

pub(super) struct ShutdownContext {
    pub(super) command_scheduler: CommandScheduler,
    pub(super) network_manager: Arc<dkg_network::NetworkManager>,
    pub(super) graceful_shutdown: GracefulShutdownConfig,
    pub(super) dkg_sync_shutdown: CancellationToken,
    pub(super) dkg_sync_handle: Option<JoinHandle<()>>,
    pub(super) periodic_shutdown: CancellationToken,
    pub(super) periodic_handle: Option<JoinHandle<()>>,
    pub(super) execute_commands_task: Option<JoinHandle<()>>,
    pub(super) network_event_loop_task: Option<JoinHandle<()>>,
    pub(super) peer_registry_shutdown: CancellationToken,
    pub(super) peer_registry_task: JoinHandle<()>,
    pub(super) http_shutdown_tx: tokio::sync::oneshot::Sender<()>,
    pub(super) handle_http_events_task: JoinHandle<()>,
}

pub(super) async fn graceful_shutdown(context: ShutdownContext) {
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
    // 8. Signal peer registry updater to stop and wait for it to exit
    // 9. Wait for HTTP to finish in-flight requests
    // 10. Shutdown complete
    let ShutdownContext {
        command_scheduler,
        network_manager,
        graceful_shutdown,
        dkg_sync_shutdown,
        dkg_sync_handle,
        periodic_shutdown,
        periodic_handle,
        execute_commands_task,
        network_event_loop_task,
        peer_registry_shutdown,
        mut peer_registry_task,
        http_shutdown_tx,
        mut handle_http_events_task,
    } = context;

    let periodic_shutdown_timeout =
        Duration::from_secs(graceful_shutdown.periodic_tasks_timeout_secs.max(1));
    let command_executor_shutdown_timeout =
        Duration::from_secs(graceful_shutdown.command_executor_timeout_secs.max(1));
    let network_shutdown_timeout =
        Duration::from_secs(graceful_shutdown.network_event_loop_timeout_secs.max(1));
    let peer_registry_shutdown_timeout =
        Duration::from_secs(graceful_shutdown.peer_registry_timeout_secs.max(1));
    let http_shutdown_timeout =
        Duration::from_secs(graceful_shutdown.http_server_timeout_secs.max(1));

    tracing::info!("Shutting down gracefully...");
    tracing::info!(
        periodic_tasks_timeout_secs = periodic_shutdown_timeout.as_secs(),
        command_executor_timeout_secs = command_executor_shutdown_timeout.as_secs(),
        network_event_loop_timeout_secs = network_shutdown_timeout.as_secs(),
        peer_registry_timeout_secs = peer_registry_shutdown_timeout.as_secs(),
        http_server_timeout_secs = http_shutdown_timeout.as_secs(),
        "Graceful shutdown timeouts"
    );

    // Step 1: Signal HTTP server to stop accepting new connections
    let _ = http_shutdown_tx.send(());

    // Step 2: Cancel periodic and DKG sync workloads (they check CancellationToken between
    // iterations)
    dkg_sync_shutdown.cancel();
    periodic_shutdown.cancel();

    // Step 3: Wait for DKG sync and periodic tasks to finish current iteration and exit
    wait_for_optional_shutdown_task("dkg_sync", periodic_shutdown_timeout, dkg_sync_handle, true)
        .await;

    wait_for_optional_shutdown_task(
        "periodic_tasks",
        periodic_shutdown_timeout,
        periodic_handle,
        true,
    )
    .await;

    // Step 4: Signal command scheduler to stop accepting new commands
    command_scheduler.shutdown();

    // Step 5: Wait for command executor to drain pending commands
    wait_for_optional_shutdown_task(
        "command_executor",
        command_executor_shutdown_timeout,
        execute_commands_task,
        true,
    )
    .await;

    // Step 6: Signal network manager event loop to stop
    network_manager.shutdown();

    // Step 7: Wait for network manager to exit
    wait_for_optional_shutdown_task(
        "network_event_loop",
        network_shutdown_timeout,
        network_event_loop_task,
        true,
    )
    .await;

    // Step 8: Signal peer registry updater to stop and wait for it to exit.
    peer_registry_shutdown.cancel();
    wait_for_shutdown_task(
        "peer_registry_updater",
        peer_registry_shutdown_timeout,
        &mut peer_registry_task,
        true,
    )
    .await;

    // Step 9: Wait for HTTP server to finish in-flight requests
    wait_for_shutdown_task(
        "http_server",
        http_shutdown_timeout,
        &mut handle_http_events_task,
        false,
    )
    .await;

    // Step 10: Shutdown complete
    tracing::info!("Shutdown complete");
}

async fn wait_for_optional_shutdown_task(
    task: &str,
    timeout: Duration,
    handle: Option<JoinHandle<()>>,
    abort_on_timeout: bool,
) {
    let Some(mut handle) = handle else {
        tracing::warn!(
            task,
            "Shutdown task already exited before coordinated shutdown"
        );
        return;
    };

    wait_for_shutdown_task(task, timeout, &mut handle, abort_on_timeout).await;
}

async fn wait_for_shutdown_task(
    task: &str,
    timeout: Duration,
    handle: &mut JoinHandle<()>,
    abort_on_timeout: bool,
) {
    match tokio::time::timeout(timeout, &mut *handle).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::error!(
            task,
            error = ?error,
            "Shutdown task panicked"
        ),
        Err(_) if abort_on_timeout => {
            tracing::warn!(
                task,
                timeout_secs = timeout.as_secs(),
                "Shutdown timeout reached, aborting task"
            );
            handle.abort();
            let _ = handle.await;
        }
        Err(_) => tracing::warn!(
            task,
            timeout_secs = timeout.as_secs(),
            "Shutdown timeout reached"
        ),
    }
}
