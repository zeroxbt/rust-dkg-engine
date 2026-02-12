use std::{future::Future, time::Duration};

use tokio_util::sync::CancellationToken;

pub(crate) async fn run_with_shutdown<F, Fut>(
    task_name: &'static str,
    shutdown: CancellationToken,
    mut run_once: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Duration>,
{
    loop {
        let delay = run_once().await;
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.cancelled() => {
                tracing::info!(task = task_name, "Periodic task shutting down");
                break;
            }
        }
    }
}
