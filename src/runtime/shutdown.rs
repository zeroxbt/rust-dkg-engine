use std::{sync::Arc, time::Duration};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::commands::scheduler::CommandScheduler;

const PERIODIC_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);
const COMMAND_EXECUTOR_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);
const NETWORK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_REGISTRY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) struct ShutdownContext {
    pub(super) command_scheduler: CommandScheduler,
    pub(super) network_manager: Arc<dkg_network::NetworkManager>,
    pub(super) periodic_shutdown: CancellationToken,
    pub(super) periodic_handle: JoinHandle<()>,
    pub(super) execute_commands_task: JoinHandle<()>,
    pub(super) network_event_loop_task: JoinHandle<()>,
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
    // 8. Wait for peer registry updater to drain
    // 9. Wait for HTTP to finish in-flight requests
    // 10. Shutdown complete
    let ShutdownContext {
        command_scheduler,
        network_manager,
        periodic_shutdown,
        mut periodic_handle,
        mut execute_commands_task,
        mut network_event_loop_task,
        mut peer_registry_task,
        http_shutdown_tx,
        mut handle_http_events_task,
    } = context;

    tracing::info!("Shutting down gracefully...");

    // Step 1: Signal HTTP server to stop accepting new connections
    let _ = http_shutdown_tx.send(());

    // Step 2: Cancel periodic tasks (they check CancellationToken between iterations)
    periodic_shutdown.cancel();

    // Step 3: Wait for periodic tasks to finish current iteration and exit
    wait_for_shutdown_task(
        "periodic_tasks",
        PERIODIC_SHUTDOWN_TIMEOUT,
        &mut periodic_handle,
        true,
    )
    .await;

    // Step 4: Signal command scheduler to stop accepting new commands
    command_scheduler.shutdown();

    // Step 5: Wait for command executor to drain pending commands
    wait_for_shutdown_task(
        "command_executor",
        COMMAND_EXECUTOR_SHUTDOWN_TIMEOUT,
        &mut execute_commands_task,
        true,
    )
    .await;

    // Step 6: Signal network manager event loop to stop
    network_manager.shutdown();

    // Step 7: Wait for network manager to exit
    wait_for_shutdown_task(
        "network_event_loop",
        NETWORK_SHUTDOWN_TIMEOUT,
        &mut network_event_loop_task,
        true,
    )
    .await;

    // Step 8: Wait for peer registry updater to drain.
    // Exits when the network broadcast sender is dropped (step 7). The timeout guards
    // against any edge case where the sender outlives the network task.
    wait_for_shutdown_task(
        "peer_registry_updater",
        PEER_REGISTRY_SHUTDOWN_TIMEOUT,
        &mut peer_registry_task,
        true,
    )
    .await;

    // Step 9: Wait for HTTP server to finish in-flight requests
    wait_for_shutdown_task(
        "http_server",
        HTTP_SHUTDOWN_TIMEOUT,
        &mut handle_http_events_task,
        false,
    )
    .await;

    // Step 10: Shutdown complete
    tracing::info!("Shutdown complete");
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
