use std::time::Duration;

use metrics::{counter, gauge, histogram};

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

pub fn record_task_cadence(task: &str, cadence: Duration) {
    histogram!(
        "node_task_cadence_seconds",
        "task" => task.to_string()
    )
    .record(cadence.as_secs_f64());
}

pub fn record_proving_stage(blockchain_id: &str, stage: &str, status: &str, duration: Duration) {
    counter!(
        "node_proving_stage_total",
        "blockchain_id" => blockchain_id.to_string(),
        "stage" => stage.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_proving_stage_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "stage" => stage.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
}

pub fn record_proving_outcome(blockchain_id: &str, outcome: &str) {
    counter!(
        "node_proving_outcome_total",
        "blockchain_id" => blockchain_id.to_string(),
        "outcome" => outcome.to_string()
    )
    .increment(1);
}

pub fn record_dial_peers_snapshot(
    total_peers: usize,
    connected_peers: usize,
    in_backoff_peers: usize,
    to_discover_peers: usize,
) {
    gauge!("node_dial_peers_total_peers").set(total_peers as f64);
    gauge!("node_dial_peers_connected_peers").set(connected_peers as f64);
    gauge!("node_dial_peers_in_backoff_peers").set(in_backoff_peers as f64);
    gauge!("node_dial_peers_to_discover_peers").set(to_discover_peers as f64);
}

pub fn record_dial_peers_discovery(status: &str, requested_peers: usize, duration: Duration) {
    counter!(
        "node_dial_peers_discovery_total",
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_dial_peers_discovery_duration_seconds",
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!(
        "node_dial_peers_discovery_requested_peers",
        "status" => status.to_string()
    )
    .record(requested_peers as f64);
}

pub fn record_process_fd_snapshot(
    total: usize,
    sockets: usize,
    pipes: usize,
    anon_inodes: usize,
    files: usize,
    other: usize,
) {
    gauge!("node_process_fd_total_snapshot").set(total as f64);
    gauge!("node_process_fd_by_type", "fd_type" => "socket").set(sockets as f64);
    gauge!("node_process_fd_by_type", "fd_type" => "pipe").set(pipes as f64);
    gauge!("node_process_fd_by_type", "fd_type" => "anon_inode").set(anon_inodes as f64);
    gauge!("node_process_fd_by_type", "fd_type" => "file").set(files as f64);
    gauge!("node_process_fd_by_type", "fd_type" => "other").set(other as f64);
}

pub fn record_sharding_table_check(
    blockchain_id: &str,
    status: &str,
    duration: Duration,
    chain_length: u128,
    local_peer_count: usize,
) {
    counter!(
        "node_sharding_table_check_total",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_sharding_table_check_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());

    gauge!(
        "node_sharding_table_chain_length",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(chain_length as f64);
    gauge!(
        "node_sharding_table_local_peer_count",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(local_peer_count as f64);
    gauge!(
        "node_sharding_table_peer_lag",
        "blockchain_id" => blockchain_id.to_string()
    )
    .set(chain_length.saturating_sub(local_peer_count as u128) as f64);
}

pub fn record_blockchain_event_listener_cycle(
    blockchain_id: &str,
    status: &str,
    duration: Duration,
    fetched_events: usize,
    processed_events: usize,
    skipped_ranges: usize,
    contracts_updated: usize,
) {
    counter!(
        "node_blockchain_event_listener_cycle_total",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_blockchain_event_listener_cycle_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());

    histogram!(
        "node_blockchain_event_listener_fetched_events",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(fetched_events as f64);
    histogram!(
        "node_blockchain_event_listener_processed_events",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(processed_events as f64);
    histogram!(
        "node_blockchain_event_listener_skipped_ranges",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(skipped_ranges as f64);
    histogram!(
        "node_blockchain_event_listener_contracts_updated",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(contracts_updated as f64);
}

#[allow(clippy::too_many_arguments)]
pub fn record_claim_rewards_cycle(
    blockchain_id: &str,
    status: &str,
    duration: Duration,
    delegators: usize,
    epochs_pending: usize,
    batches_attempted: usize,
    batches_succeeded: usize,
    batches_failed: usize,
) {
    counter!(
        "node_claim_rewards_cycle_total",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_claim_rewards_cycle_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!(
        "node_claim_rewards_cycle_delegators",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(delegators as f64);
    histogram!(
        "node_claim_rewards_cycle_epochs_pending",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(epochs_pending as f64);
    histogram!(
        "node_claim_rewards_cycle_batches_attempted",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(batches_attempted as f64);
    histogram!(
        "node_claim_rewards_cycle_batches_succeeded",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(batches_succeeded as f64);
    histogram!(
        "node_claim_rewards_cycle_batches_failed",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(batches_failed as f64);
}

pub fn record_claim_rewards_batch(
    blockchain_id: &str,
    status: &str,
    batch_size: usize,
    duration: Duration,
) {
    counter!(
        "node_claim_rewards_batch_total",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
    histogram!(
        "node_claim_rewards_batch_duration_seconds",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
    histogram!(
        "node_claim_rewards_batch_size",
        "blockchain_id" => blockchain_id.to_string(),
        "status" => status.to_string()
    )
    .record(batch_size as f64);
}
