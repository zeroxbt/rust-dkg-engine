use std::time::Duration;

use metrics::{counter, gauge, histogram};

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
