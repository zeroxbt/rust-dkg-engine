use std::time::Duration;

use metrics::{counter, histogram};

pub fn record_operation_duration(operation: &str, status: &str, duration: Duration) {
    histogram!(
        "node_operation_duration_seconds",
        "operation" => operation.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
}

pub fn record_publish_finalization_total(blockchain_id: &str) {
    counter!(
        "node_publish_finalization_total",
        "blockchain_id" => blockchain_id.to_string()
    )
    .increment(1);
}

pub fn record_publish_finalization_duration(blockchain_id: &str, duration: Duration) {
    histogram!(
        "node_publish_finalization_duration_seconds",
        "blockchain_id" => blockchain_id.to_string()
    )
    .record(duration.as_secs_f64());
}
