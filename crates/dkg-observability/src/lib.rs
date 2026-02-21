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

pub fn record_network_action_enqueue(queue: &str, action: &str, status: &str, wait: Duration) {
    counter!(
        "node_network_action_enqueue_total",
        "queue" => queue.to_string(),
        "action" => action.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_network_action_enqueue_wait_seconds",
        "queue" => queue.to_string(),
        "action" => action.to_string(),
        "status" => status.to_string()
    )
    .record(wait.as_secs_f64());
}

pub fn record_network_action_dequeue(queue: &str, action: &str) {
    counter!(
        "node_network_action_dequeue_total",
        "queue" => queue.to_string(),
        "action" => action.to_string()
    )
    .increment(1);
}

pub fn record_network_action_queue_depth(queue: &str, depth: usize) {
    gauge!(
        "node_network_action_queue_depth",
        "queue" => queue.to_string()
    )
    .set(depth as f64);
}

pub fn record_network_pending_requests(protocol: &str, pending: usize) {
    gauge!(
        "node_network_pending_requests",
        "protocol" => protocol.to_string()
    )
    .set(pending as f64);
}

pub fn record_network_response_send(protocol: &str, mode: &str, status: &str) {
    counter!(
        "node_network_response_send_total",
        "protocol" => protocol.to_string(),
        "mode" => mode.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

pub fn record_peer_registry_population(
    blockchain_id: &str,
    known_peers: usize,
    shard_peers: usize,
    identified_shard_peers: usize,
) {
    gauge!("node_peer_registry_known_peers").set(known_peers as f64);
    gauge!(
        "node_peer_registry_shard_peers",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(shard_peers as f64);
    gauge!(
        "node_peer_registry_identified_shard_peers",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(identified_shard_peers as f64);
}

pub fn record_peer_registry_protocol_capable_shard_peers(
    blockchain_id: &str,
    protocol: &str,
    protocol_capable_peers: usize,
) {
    gauge!(
        "node_peer_registry_protocol_capable_shard_peers",
        "blockchain_id" => blockchain_id.to_string(),
        "protocol" => protocol.to_string()
    )
    .set(protocol_capable_peers as f64);
}

pub fn record_peer_registry_selection(
    blockchain_id: &str,
    protocol: &str,
    shard_peers: usize,
    protocol_capable_peers: usize,
) {
    histogram!(
        "node_peer_registry_select_shard_peers",
        "blockchain_id" => blockchain_id.to_string(),
        "protocol" => protocol.to_string()
    )
    .record(shard_peers as f64);
    histogram!(
        "node_peer_registry_select_protocol_capable_peers",
        "blockchain_id" => blockchain_id.to_string(),
        "protocol" => protocol.to_string()
    )
    .record(protocol_capable_peers as f64);
}

pub fn record_peer_registry_discovery_backoff(active_peers: usize) {
    gauge!("node_peer_registry_discovery_backoff_active").set(active_peers as f64);
}

pub fn record_peer_registry_request_outcome(protocol: &str, outcome: &str, duration: Duration) {
    counter!(
        "node_peer_registry_request_outcome_total",
        "protocol" => protocol.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
    histogram!(
        "node_peer_registry_request_outcome_duration_seconds",
        "protocol" => protocol.to_string(),
        "outcome" => outcome.to_string()
    )
    .record(duration.as_secs_f64());
}

#[allow(clippy::too_many_arguments)]
pub fn record_sync_queue_snapshot(
    blockchain_id: &str,
    queue_total: u64,
    queue_due: u64,
    queue_retrying: u64,
    oldest_due_age_secs: u64,
    progress_tracked_contracts: u64,
    progress_last_update_age_secs: u64,
) {
    gauge!(
        "node_sync_queue_total",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(queue_total as f64);
    gauge!(
        "node_sync_queue_due",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(queue_due as f64);
    gauge!(
        "node_sync_queue_retrying",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(queue_retrying as f64);
    gauge!(
        "node_sync_queue_oldest_due_age_seconds",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(oldest_due_age_secs as f64);
    gauge!(
        "node_sync_progress_tracked_contracts",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(progress_tracked_contracts as f64);
    gauge!(
        "node_sync_progress_last_update_age_seconds",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(progress_last_update_age_secs as f64);
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

pub fn record_sync_filter_batch(
    duration: Duration,
    batch_kcs: usize,
    to_sync_kcs: usize,
    already_synced_kcs: usize,
    expired_kcs: usize,
) {
    counter!("node_sync_filter_batch_total").increment(1);
    histogram!("node_sync_filter_batch_duration_seconds").record(duration.as_secs_f64());
    histogram!("node_sync_filter_batch_kcs").record(batch_kcs as f64);
    histogram!("node_sync_filter_batch_to_sync_kcs").record(to_sync_kcs as f64);
    histogram!("node_sync_filter_batch_already_synced_kcs").record(already_synced_kcs as f64);
    histogram!("node_sync_filter_batch_expired_kcs").record(expired_kcs as f64);

    counter!("node_sync_filter_kcs_total", "result" => "to_sync".to_string())
        .increment(to_sync_kcs as u64);
    counter!(
        "node_sync_filter_kcs_total",
        "result" => "already_synced".to_string()
    )
    .increment(already_synced_kcs as u64);
    counter!("node_sync_filter_kcs_total", "result" => "expired".to_string())
        .increment(expired_kcs as u64);
}

pub fn record_sync_filter_rpc(status: &str, duration: Duration, kc_count: usize) {
    counter!(
        "node_sync_filter_rpc_total",
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_sync_filter_rpc_duration_seconds",
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!("node_sync_filter_rpc_kcs", "status" => status.to_string()).record(kc_count as f64);
}

pub fn record_sync_insert_batch(
    status: &str,
    duration: Duration,
    batch_kcs: usize,
    synced_kcs: usize,
    failed_kcs: usize,
) {
    counter!(
        "node_sync_insert_batch_total",
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_sync_insert_batch_duration_seconds",
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!(
        "node_sync_insert_batch_kcs",
        "status" => status.to_string()
    )
    .record(batch_kcs as f64);
    histogram!(
        "node_sync_insert_batch_synced_kcs",
        "status" => status.to_string()
    )
    .record(synced_kcs as f64);
    histogram!(
        "node_sync_insert_batch_failed_kcs",
        "status" => status.to_string()
    )
    .record(failed_kcs as f64);

    counter!("node_sync_insert_kcs_total", "status" => "synced".to_string())
        .increment(synced_kcs as u64);
    counter!("node_sync_insert_kcs_total", "status" => "failed".to_string())
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
