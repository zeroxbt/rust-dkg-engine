use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dkg_network::RequestOutcomeKind;
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

pub(crate) fn record_network_outbound_request(
    protocol: &str,
    outcome: RequestOutcomeKind,
    duration: Duration,
) {
    let outcome = match outcome {
        RequestOutcomeKind::Success => "success",
        RequestOutcomeKind::ResponseError => "response_error",
        RequestOutcomeKind::Failure => "failure",
    };
    counter!(
        "node_network_outbound_request_total",
        "protocol" => protocol.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
    histogram!(
        "node_network_outbound_request_duration_seconds",
        "protocol" => protocol.to_string(),
        "outcome" => outcome.to_string()
    )
    .record(duration.as_secs_f64());
}

pub(crate) fn record_network_peer_event(event: &str, status: &str) {
    counter!(
        "node_network_peer_event_total",
        "event" => event.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

pub(crate) fn record_network_inbound_request(protocol: &str, decision: &str) {
    counter!(
        "node_network_inbound_request_total",
        "protocol" => protocol.to_string(),
        "decision" => decision.to_string()
    )
    .increment(1);
}

pub(crate) fn record_sync_fetch_peer_selection(
    blockchain_id: &str,
    shard_members: usize,
    identified: usize,
    usable: usize,
) {
    gauge!(
        "node_sync_fetch_shard_members",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(shard_members as f64);
    gauge!(
        "node_sync_fetch_identified_peers",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(identified as f64);
    gauge!(
        "node_sync_fetch_usable_peers",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(usable as f64);
}

pub(crate) fn record_sync_fetch_peer_request(status: &str, duration: Duration, valid_kcs: usize) {
    counter!(
        "node_sync_fetch_peer_request_total",
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_sync_fetch_peer_request_duration_seconds",
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!(
        "node_sync_fetch_peer_valid_kcs",
        "status" => status.to_string()
    )
    .record(valid_kcs as f64);
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn record_sync_fetch_batch(
    status: &str,
    duration: Duration,
    batch_kcs: usize,
    batch_assets: u64,
    fetched_kcs: usize,
    failed_kcs: usize,
) {
    counter!(
        "node_sync_fetch_batch_total",
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_sync_fetch_batch_duration_seconds",
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!(
        "node_sync_fetch_batch_kcs",
        "status" => status.to_string()
    )
    .record(batch_kcs as f64);
    histogram!(
        "node_sync_fetch_batch_assets",
        "status" => status.to_string()
    )
    .record(batch_assets as f64);

    counter!("node_sync_fetch_kcs_total", "status" => "fetched".to_string())
        .increment(fetched_kcs as u64);
    counter!("node_sync_fetch_kcs_total", "status" => "failed".to_string())
        .increment(failed_kcs as u64);
}
