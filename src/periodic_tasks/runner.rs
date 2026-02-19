use std::{future::Future, time::Duration};

use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use crate::observability;

pub(crate) async fn run_with_shutdown<F, Fut>(
    task_name: &'static str,
    shutdown: CancellationToken,
    mut run_once: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Duration>,
{
    loop {
        let started_at = std::time::Instant::now();
        let run_once_result = std::panic::AssertUnwindSafe(run_once())
            .catch_unwind()
            .await;
        let delay = match run_once_result {
            Ok(delay) => {
                observability::record_task_run(task_name, "completed", started_at.elapsed());
                delay
            }
            Err(payload) => {
                observability::record_task_run(task_name, "panicked", started_at.elapsed());
                std::panic::resume_unwind(payload);
            }
        };

        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.cancelled() => {
                tracing::info!(task = task_name, "Periodic task shutting down");
                break;
            }
        }
    }
}
