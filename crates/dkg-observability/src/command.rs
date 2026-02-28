use std::time::Duration;

use metrics::{counter, gauge, histogram};

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

pub fn record_command_execution_delay(command: &str, status: &str, delay: Duration) {
    histogram!(
        "node_command_execution_delay_seconds",
        "command" => command.to_string(),
        "status" => status.to_string()
    )
    .record(delay.as_secs_f64());
}

pub fn record_command_queue_depth(depth: usize) {
    gauge!("node_command_queue_depth").set(depth as f64);
}

pub fn record_command_queue_fill_ratio(fill_ratio: f64) {
    gauge!("node_command_queue_fill_ratio").set(fill_ratio.clamp(0.0, 1.0));
}
