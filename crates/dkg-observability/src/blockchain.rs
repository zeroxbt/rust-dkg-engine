use std::time::Duration;

use metrics::{counter, histogram};

pub fn record_blockchain_rpc_call(
    blockchain_id: &str,
    operation: &str,
    status: &str,
    duration: Duration,
) {
    counter!(
        "node_blockchain_rpc_total",
        "blockchain_id" => blockchain_id.to_string(),
        "operation" => operation.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_blockchain_rpc_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "operation" => operation.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
}

pub fn record_blockchain_rpc_retry(blockchain_id: &str, operation: &str) {
    counter!(
        "node_blockchain_rpc_retries_total",
        "blockchain_id" => blockchain_id.to_string(),
        "operation" => operation.to_string()
    )
    .increment(1);
}

pub fn record_blockchain_tx_stage(
    blockchain_id: &str,
    operation: &str,
    stage: &str,
    status: &str,
    duration: Duration,
) {
    counter!(
        "node_blockchain_tx_stage_total",
        "blockchain_id" => blockchain_id.to_string(),
        "operation" => operation.to_string(),
        "stage" => stage.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_blockchain_tx_stage_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "operation" => operation.to_string(),
        "stage" => stage.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
}

pub fn record_blockchain_tx_retry(blockchain_id: &str, operation: &str, stage: &str) {
    counter!(
        "node_blockchain_tx_retries_total",
        "blockchain_id" => blockchain_id.to_string(),
        "operation" => operation.to_string(),
        "stage" => stage.to_string()
    )
    .increment(1);
}

pub fn record_blockchain_event_logs_batch(
    blockchain_id: &str,
    contract: &str,
    status: &str,
    duration: Duration,
    block_span: usize,
    logs: usize,
) {
    counter!(
        "node_blockchain_event_logs_batch_total",
        "blockchain_id" => blockchain_id.to_string(),
        "contract" => contract.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_blockchain_event_logs_batch_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "contract" => contract.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!(
        "node_blockchain_event_logs_batch_blocks",
        "blockchain_id" => blockchain_id.to_string(),
        "contract" => contract.to_string(),
        "status" => status.to_string()
    )
    .record(block_span as f64);
    histogram!(
        "node_blockchain_event_logs_batch_events",
        "blockchain_id" => blockchain_id.to_string(),
        "contract" => contract.to_string(),
        "status" => status.to_string()
    )
    .record(logs as f64);
}
