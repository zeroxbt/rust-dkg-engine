use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

pub fn record_task_run(task: &str, status: &str, duration: Duration) {
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

pub fn record_sync_last_success_heartbeat() {
    let unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    gauge!("node_sync_last_success_unix").set(unix_seconds);
}

pub fn record_network_outbound_request(protocol: &str, outcome: &str, duration: Duration) {
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

pub fn record_network_peer_event(event: &str, status: &str) {
    counter!(
        "node_network_peer_event_total",
        "event" => event.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

pub fn record_network_inbound_request(protocol: &str, decision: &str) {
    counter!(
        "node_network_inbound_request_total",
        "protocol" => protocol.to_string(),
        "decision" => decision.to_string()
    )
    .increment(1);
}

pub fn record_sync_fetch_peer_selection(
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

pub fn record_sync_fetch_peer_request(status: &str, duration: Duration, valid_kcs: usize) {
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
pub fn record_sync_fetch_batch(
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

pub fn record_triple_store_backend_query_bytes_total(backend: &str, op: &str, bytes: usize) {
    counter!(
        "node_triple_store_backend_query_bytes_total",
        "backend" => backend.to_string(),
        "op" => op.to_string()
    )
    .increment(bytes as u64);
}

pub fn record_triple_store_backend_result_bytes_total(backend: &str, op: &str, bytes: usize) {
    counter!(
        "node_triple_store_backend_result_bytes_total",
        "backend" => backend.to_string(),
        "op" => op.to_string()
    )
    .increment(bytes as u64);
}

pub fn record_triple_store_backend_permit_wait(backend: &str, op: &str, wait: Duration) {
    histogram!(
        "node_triple_store_backend_permit_wait_seconds",
        "backend" => backend.to_string(),
        "op" => op.to_string()
    )
    .record(wait.as_secs_f64());
}

pub fn record_triple_store_backend_permit_snapshot(backend: &str, max: usize, available: usize) {
    gauge!(
        "node_triple_store_backend_available_permits",
        "backend" => backend.to_string()
    )
    .set(available as f64);

    gauge!(
        "node_triple_store_backend_in_use_permits",
        "backend" => backend.to_string()
    )
    .set(max.saturating_sub(available) as f64);
}

pub fn record_triple_store_backend_operation(
    backend: &str,
    op: &str,
    status: &str,
    error_class: &str,
    duration: Duration,
) {
    counter!(
        "node_triple_store_backend_operation_total",
        "backend" => backend.to_string(),
        "op" => op.to_string(),
        "status" => status.to_string(),
        "error_class" => error_class.to_string()
    )
    .increment(1);

    histogram!(
        "node_triple_store_backend_operation_duration_seconds",
        "backend" => backend.to_string(),
        "op" => op.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
}

#[allow(clippy::too_many_arguments)]
pub fn record_triple_store_kc_insert(
    backend: &str,
    status: &str,
    error_class: &str,
    size_bucket: &str,
    ka_bucket: &str,
    triples_bucket: &str,
    has_private: bool,
    has_metadata: bool,
    has_paranet: bool,
    duration: Duration,
    raw_bytes: usize,
    inserted_triples: usize,
) {
    counter!(
        "node_triple_store_kc_insert_total",
        "backend" => backend.to_string(),
        "status" => status.to_string(),
        "error_class" => error_class.to_string(),
        "size_bucket" => size_bucket.to_string(),
        "ka_bucket" => ka_bucket.to_string(),
        "triples_bucket" => triples_bucket.to_string(),
        "has_private" => bool_label(has_private).to_string(),
        "has_metadata" => bool_label(has_metadata).to_string(),
        "has_paranet" => bool_label(has_paranet).to_string()
    )
    .increment(1);

    histogram!(
        "node_triple_store_kc_insert_duration_seconds",
        "backend" => backend.to_string(),
        "status" => status.to_string(),
        "size_bucket" => size_bucket.to_string(),
        "ka_bucket" => ka_bucket.to_string(),
        "triples_bucket" => triples_bucket.to_string()
    )
    .record(duration.as_secs_f64());

    histogram!(
        "node_triple_store_kc_insert_raw_bytes",
        "backend" => backend.to_string(),
        "status" => status.to_string(),
        "size_bucket" => size_bucket.to_string(),
        "ka_bucket" => ka_bucket.to_string()
    )
    .record(raw_bytes as f64);

    histogram!(
        "node_triple_store_kc_insert_triples",
        "backend" => backend.to_string(),
        "status" => status.to_string(),
        "triples_bucket" => triples_bucket.to_string()
    )
    .record(inserted_triples as f64);
}

#[allow(clippy::too_many_arguments)]
pub fn record_triple_store_query_operation(
    backend: &str,
    query_kind: &str,
    visibility: &str,
    status: &str,
    error_class: &str,
    asset_bucket: &str,
    requested_uals_bucket: &str,
    duration: Duration,
    result_bytes: Option<usize>,
    result_triples: Option<usize>,
) {
    counter!(
        "node_triple_store_query_total",
        "backend" => backend.to_string(),
        "query_kind" => query_kind.to_string(),
        "visibility" => visibility.to_string(),
        "status" => status.to_string(),
        "error_class" => error_class.to_string(),
        "asset_bucket" => asset_bucket.to_string(),
        "requested_uals_bucket" => requested_uals_bucket.to_string()
    )
    .increment(1);

    histogram!(
        "node_triple_store_query_duration_seconds",
        "backend" => backend.to_string(),
        "query_kind" => query_kind.to_string(),
        "visibility" => visibility.to_string(),
        "status" => status.to_string(),
        "asset_bucket" => asset_bucket.to_string(),
        "requested_uals_bucket" => requested_uals_bucket.to_string()
    )
    .record(duration.as_secs_f64());

    if let Some(result_bytes) = result_bytes {
        histogram!(
            "node_triple_store_query_result_bytes",
            "backend" => backend.to_string(),
            "query_kind" => query_kind.to_string(),
            "visibility" => visibility.to_string(),
            "asset_bucket" => asset_bucket.to_string(),
            "requested_uals_bucket" => requested_uals_bucket.to_string()
        )
        .record(result_bytes as f64);
    }

    if let Some(result_triples) = result_triples {
        histogram!(
            "node_triple_store_query_result_triples",
            "backend" => backend.to_string(),
            "query_kind" => query_kind.to_string(),
            "visibility" => visibility.to_string(),
            "asset_bucket" => asset_bucket.to_string(),
            "requested_uals_bucket" => requested_uals_bucket.to_string()
        )
        .record(result_triples as f64);
    }
}

fn bool_label(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}
