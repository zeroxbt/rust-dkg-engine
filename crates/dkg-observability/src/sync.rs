use std::time::Duration;

use metrics::{gauge, histogram};

pub fn record_sync_queue_snapshot(
    blockchain_id: &str,
    queue_total: u64,
    queue_due: u64,
    queue_retrying: u64,
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
}

pub fn record_sync_metadata_total_snapshot(blockchain_id: &str, metadata_kcs_total: u64) {
    gauge!(
        "node_sync_metadata_kcs_total",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(metadata_kcs_total as f64);
}

pub fn record_sync_metadata_backfill_total_snapshot(
    blockchain_id: &str,
    metadata_backfill_kcs_total: u64,
) {
    gauge!(
        "node_sync_metadata_backfill_kcs_total",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(metadata_backfill_kcs_total as f64);
}

pub fn record_sync_metadata_backfill_batch(
    blockchain_id: &str,
    status: &str,
    blocks_scanned: u64,
    events_found: usize,
) {
    histogram!(
        "node_sync_metadata_backfill_batch_blocks",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(blocks_scanned as f64);
    histogram!(
        "node_sync_metadata_backfill_batch_events",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(events_found as f64);
}

pub fn record_sync_fetch_batch(
    status: &str,
    duration: Duration,
    batch_kcs: usize,
    batch_assets: u64,
) {
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
}

pub fn record_sync_insert_batch(
    status: &str,
    duration: Duration,
    batch_kcs: usize,
    batch_assets: u64,
) {
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
        "node_sync_insert_batch_assets",
        "status" => status.to_string()
    )
    .record(batch_assets as f64);
}
