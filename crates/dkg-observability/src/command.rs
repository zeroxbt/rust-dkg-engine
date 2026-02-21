use std::time::Duration;

use metrics::{counter, histogram};

pub fn record_command_total(command: &str, status: &str) {
    counter!(
        "node_command_total",
        "command" => command.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

pub fn record_command_duration(command: &str, status: &str, duration: Duration) {
    histogram!(
        "node_command_duration_seconds",
        "command" => command.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
}
