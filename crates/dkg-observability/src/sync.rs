use std::time::{Duration, SystemTime, UNIX_EPOCH};

use metrics::{counter, gauge, histogram};

pub fn record_sync_last_success_heartbeat() {
    let unix_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    gauge!("node_sync_last_success_unix").set(unix_seconds);
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

#[allow(clippy::too_many_arguments)]
pub fn record_sync_cycle(
    blockchain_id: &str,
    status: &str,
    duration: Duration,
    contracts: usize,
    enqueued: u64,
    pending: usize,
    synced: u64,
    failed: u64,
) {
    counter!(
        "node_sync_cycle_total",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_sync_cycle_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!(
        "node_sync_cycle_contracts",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(contracts as f64);
    histogram!(
        "node_sync_cycle_enqueued_kcs",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(enqueued as f64);
    histogram!(
        "node_sync_cycle_pending_kcs",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(pending as f64);
    histogram!(
        "node_sync_cycle_synced_kcs",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(synced as f64);
    histogram!(
        "node_sync_cycle_failed_kcs",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(failed as f64);
}

pub fn record_sync_cycle_rss_bytes(blockchain_id: &str, phase: &str, bytes: u64) {
    gauge!(
        "node_sync_cycle_rss_bytes",
        "blockchain_id" => blockchain_id.to_string(),
        "phase" => phase.to_string()
    )
    .set(bytes as f64);
}

pub fn record_sync_cycle_rss_delta_bytes(blockchain_id: &str, delta_bytes: i64) {
    gauge!(
        "node_sync_cycle_rss_delta_bytes",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(delta_bytes as f64);
    histogram!(
        "node_sync_cycle_rss_delta_abs_bytes",
        "blockchain_id" => blockchain_id.to_string()
    )
    .record(delta_bytes.unsigned_abs() as f64);
}

pub fn record_sync_pipeline_channel_depth(blockchain_id: &str, channel: &str, depth: usize) {
    gauge!(
        "node_sync_pipeline_channel_depth",
        "blockchain_id" => blockchain_id.to_string(),
        "channel" => channel.to_string()
    )
    .set(depth as f64);
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

pub fn record_sync_fetch_batch_payload_bytes(status: &str, payload_bytes: u64, fetched_kcs: usize) {
    histogram!(
        "node_sync_fetch_batch_payload_bytes",
        "status" => status.to_string()
    )
    .record(payload_bytes as f64);
    if fetched_kcs > 0 {
        histogram!(
            "node_sync_fetch_kc_payload_bytes",
            "status" => status.to_string()
        )
        .record(payload_bytes as f64 / fetched_kcs as f64);
    }
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

pub fn record_sync_metadata_backfill_chunk(
    blockchain_id: &str,
    status: &str,
    range_type: &str,
    duration: Duration,
    blocks_scanned: u64,
    events_found: usize,
) {
    counter!(
        "node_sync_metadata_backfill_chunk_total",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string(),
        "range_type" => range_type.to_string()
    )
    .increment(1);
    histogram!(
        "node_sync_metadata_backfill_chunk_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string(),
        "range_type" => range_type.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!(
        "node_sync_metadata_backfill_blocks_scanned",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string(),
        "range_type" => range_type.to_string()
    )
    .record(blocks_scanned as f64);
    histogram!(
        "node_sync_metadata_backfill_events_found",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string(),
        "range_type" => range_type.to_string()
    )
    .record(events_found as f64);
}

pub fn record_sync_metadata_events_found_total(blockchain_id: &str, events_found: u64) {
    counter!(
        "node_sync_metadata_events_found_total",
        "blockchain_id" => blockchain_id.to_string()
    )
    .increment(events_found);
}

pub fn record_sync_metadata_cursor(blockchain_id: &str, contract: &str, cursor_block: u64) {
    gauge!(
        "node_sync_metadata_cursor_block",
        "blockchain_id" => blockchain_id.to_string(),
        "contract" => contract.to_string()
    )
    .set(cursor_block as f64);
}

pub fn record_sync_metadata_gaps_detected(blockchain_id: &str, contract: &str, count: u64) {
    gauge!(
        "node_sync_metadata_gaps_detected",
        "blockchain_id" => blockchain_id.to_string(),
        "contract" => contract.to_string()
    )
    .set(count as f64);
}

pub fn record_sync_metadata_cursor_lag(blockchain_id: &str, contract: &str, lag_blocks: u64) {
    gauge!(
        "node_sync_metadata_cursor_lag_blocks",
        "blockchain_id" => blockchain_id.to_string(),
        "contract" => contract.to_string()
    )
    .set(lag_blocks as f64);
}

pub fn record_kc_state_metadata_hydration_batch(
    blockchain_id: &str,
    status: &str,
    duration: Duration,
    kc_count: usize,
) {
    counter!(
        "node_kc_state_metadata_hydration_batch_total",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_kc_state_metadata_hydration_batch_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!(
        "node_kc_state_metadata_hydration_batch_kcs",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(kc_count as f64);
}

pub fn record_kc_state_metadata_ready_count(blockchain_id: &str, count: usize) {
    gauge!(
        "node_kc_state_metadata_ready_count",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(count as f64);
}

pub fn record_sync_waiting_for_core_metadata_count(blockchain_id: &str, count: usize) {
    gauge!(
        "node_sync_waiting_for_core_metadata_count",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(count as f64);
}

pub fn record_sync_waiting_for_kc_state_metadata_count(blockchain_id: &str, count: usize) {
    gauge!(
        "node_sync_waiting_for_kc_state_metadata_count",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(count as f64);
}

pub fn record_kc_state_metadata_observed_lag(blockchain_id: &str, contract: &str, lag_blocks: u64) {
    gauge!(
        "node_kc_state_metadata_observed_lag_blocks",
        "blockchain_id" => blockchain_id.to_string(),
        "contract" => contract.to_string()
    )
    .set(lag_blocks as f64);
}
