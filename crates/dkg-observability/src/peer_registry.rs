use std::time::Duration;

use metrics::{counter, gauge, histogram};

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
