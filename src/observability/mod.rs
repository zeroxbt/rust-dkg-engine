use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metrics::{counter, gauge, histogram};

pub(crate) fn record_command_total(command: &str, status: &str) {
    counter!(
        "node_command_total",
        "command" => command.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

pub(crate) fn record_command_duration(command: &str, status: &str, duration: Duration) {
    histogram!(
        "node_command_duration_seconds",
        "command" => command.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
}

pub(crate) fn record_task_run(task: &str, status: &str, duration: Duration) {
    counter!(
        "node_task_runs_total",
        "task" => task.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_task_duration_seconds",
        "task" => task.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
}

pub(crate) fn record_sync_last_success_heartbeat() {
    let unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    gauge!("node_sync_last_success_unix").set(unix_seconds);
}
