use std::{sync::Arc, time::Duration};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{commands::scheduler::CommandScheduler, logger};

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
    // 10. Flush telemetry
    let ShutdownContext {
        command_scheduler,
        network_manager,
        periodic_shutdown,
        mut periodic_handle,
        mut execute_commands_task,
        mut network_event_loop_task,
        mut peer_registry_task,
        http_shutdown_tx,
        handle_http_events_task,
    } = context;

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
    match tokio::time::timeout(NETWORK_SHUTDOWN_TIMEOUT, &mut network_event_loop_task).await {
        Ok(Ok(())) => tracing::info!("Network manager shut down cleanly"),
        Ok(Err(e)) => tracing::error!("Network task panicked: {:?}", e),
        Err(_) => {
            tracing::warn!(
                timeout_secs = NETWORK_SHUTDOWN_TIMEOUT.as_secs(),
                "Network manager shutdown timeout, aborting"
            );
            network_event_loop_task.abort();
            let _ = network_event_loop_task.await;
        }
    }

    // Step 8: Wait for peer registry updater to drain.
    // Exits when the network broadcast sender is dropped (step 7). The timeout guards
    // against any edge case where the sender outlives the network task.
    match tokio::time::timeout(PEER_REGISTRY_SHUTDOWN_TIMEOUT, &mut peer_registry_task).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::error!("Peer registry updater panicked: {:?}", e),
        Err(_) => {
            tracing::warn!("Peer registry updater did not terminate in time, aborting");
            peer_registry_task.abort();
            let _ = peer_registry_task.await;
        }
    }

    // Step 9: Wait for HTTP server to finish in-flight requests
    tracing::info!("Waiting for HTTP server to shut down...");
    match tokio::time::timeout(HTTP_SHUTDOWN_TIMEOUT, handle_http_events_task).await {
        Ok(Ok(())) => tracing::info!("HTTP server shut down cleanly"),
        Ok(Err(e)) => tracing::error!("HTTP task panicked: {:?}", e),
        Err(_) => tracing::warn!(
            timeout_secs = HTTP_SHUTDOWN_TIMEOUT.as_secs(),
            "HTTP server shutdown timeout"
        ),
    }

    // Step 10: Flush OpenTelemetry traces
    logger::shutdown_telemetry();
    tracing::info!("Shutdown complete");
}
