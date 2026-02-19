use std::time::Duration;

use dkg_domain::BlockchainId;
use dkg_network::{
    IdentifyInfo, PROTOCOL_NAME_BATCH_GET, PeerEvent, PeerId, RequestOutcome, RequestOutcomeKind,
    StreamProtocol,
};

use super::config::{DEFAULT_LATENCY_MS, FAILURE_LATENCY_MS};
use super::*;

const TEST_BLOCKCHAIN: &str = "test:chain";

/// Helper: register a peer in the registry via shard membership.
fn register_peer(registry: &PeerRegistry, peer: PeerId) {
    let blockchain = BlockchainId::from(TEST_BLOCKCHAIN.to_string());
    registry.set_shard_membership(&blockchain, &[peer]);
}

#[test]
fn test_unknown_peer_gets_default_latency() {
    let registry = PeerRegistry::new();
    let peer = PeerId::random();
    assert_eq!(registry.get_average_latency(&peer), DEFAULT_LATENCY_MS);
}

#[test]
fn test_record_latency() {
    let registry = PeerRegistry::new();
    let peer = PeerId::random();
    register_peer(&registry, peer);

    registry.record_latency(peer, Duration::from_millis(100));
    assert_eq!(registry.get_average_latency(&peer), 100);

    registry.record_latency(peer, Duration::from_millis(200));
    assert_eq!(registry.get_average_latency(&peer), 150);
}

#[test]
fn test_latency_ignored_for_unknown_peer() {
    let registry = PeerRegistry::new();
    let peer = PeerId::random();

    registry.record_latency(peer, Duration::from_millis(100));
    // Peer was never registered, so the latency was silently dropped
    assert_eq!(registry.get_average_latency(&peer), DEFAULT_LATENCY_MS);
}

#[test]
fn test_failure_records_high_latency() {
    let registry = PeerRegistry::new();
    let peer = PeerId::random();
    register_peer(&registry, peer);

    registry.record_request_failure(peer);
    assert_eq!(registry.get_average_latency(&peer), FAILURE_LATENCY_MS);
}

#[test]
fn test_mixed_success_and_failure() {
    let registry = PeerRegistry::new();
    let peer = PeerId::random();
    register_peer(&registry, peer);

    registry.record_latency(peer, Duration::from_millis(1000));
    registry.record_request_failure(peer);
    assert_eq!(registry.get_average_latency(&peer), 8000);
}

#[test]
fn test_ring_buffer_overwrites_old_entries() {
    let registry = PeerRegistry::new();
    let peer = PeerId::random();
    register_peer(&registry, peer);

    for _ in 0..20 {
        registry.record_latency(peer, Duration::from_millis(100));
    }
    assert_eq!(registry.get_average_latency(&peer), 100);

    for _ in 0..10 {
        registry.record_latency(peer, Duration::from_millis(200));
    }
    assert_eq!(registry.get_average_latency(&peer), 150);

    for _ in 0..10 {
        registry.record_latency(peer, Duration::from_millis(200));
    }
    assert_eq!(registry.get_average_latency(&peer), 200);
}

#[test]
fn test_sort_by_latency() {
    let registry = PeerRegistry::new();

    let fast_peer = PeerId::random();
    let slow_peer = PeerId::random();
    let failing_peer = PeerId::random();
    let unknown_peer = PeerId::random();

    register_peer(&registry, fast_peer);
    register_peer(&registry, slow_peer);
    register_peer(&registry, failing_peer);

    registry.record_latency(fast_peer, Duration::from_millis(500));
    registry.record_latency(fast_peer, Duration::from_millis(500));

    registry.record_latency(slow_peer, Duration::from_millis(8000));
    registry.record_latency(slow_peer, Duration::from_millis(8000));

    registry.record_request_failure(failing_peer);
    registry.record_request_failure(failing_peer);

    let mut peers = vec![failing_peer, unknown_peer, slow_peer, fast_peer];
    registry.sort_by_latency(&mut peers);

    assert_eq!(peers[0], fast_peer);
    assert_eq!(peers[1], unknown_peer);
    assert_eq!(peers[2], slow_peer);
    assert_eq!(peers[3], failing_peer);
}

#[test]
fn test_discovery_backoff() {
    let registry = PeerRegistry::new();
    let peer = PeerId::random();
    register_peer(&registry, peer);

    // Known peer with no failures - should attempt
    assert!(registry.should_attempt_discovery(&peer));

    // After failure - should not attempt (backoff active)
    registry.record_discovery_failure(peer);
    assert!(!registry.should_attempt_discovery(&peer));

    // Check backoff duration exists
    assert!(registry.get_discovery_backoff(&peer).is_some());

    // After success - should attempt again
    registry.record_discovery_success(peer);
    assert!(registry.should_attempt_discovery(&peer));
    assert!(registry.get_discovery_backoff(&peer).is_none());
}

#[test]
fn test_exponential_backoff_calculation() {
    let registry = PeerRegistry::new();

    assert_eq!(registry.calculate_backoff(0), Duration::ZERO);
    assert_eq!(registry.calculate_backoff(1), Duration::from_secs(60));
    assert_eq!(registry.calculate_backoff(2), Duration::from_secs(120));
    assert_eq!(registry.calculate_backoff(3), Duration::from_secs(240));
    assert_eq!(registry.calculate_backoff(4), Duration::from_secs(480));
    assert_eq!(registry.calculate_backoff(5), Duration::from_secs(960));
    // Should cap at max
    assert_eq!(registry.calculate_backoff(10), Duration::from_secs(960));
}

#[test]
fn test_apply_peer_event_updates_identify() {
    let registry = PeerRegistry::new();
    let blockchain = BlockchainId::from(TEST_BLOCKCHAIN.to_string());
    let peer = PeerId::random();
    register_peer(&registry, peer);

    registry.apply_peer_event(PeerEvent::IdentifyReceived {
        peer_id: peer,
        info: IdentifyInfo {
            protocols: vec![StreamProtocol::new("/my-protocol/1.0.0")],
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/4100".parse().expect("valid addr")],
        },
    });

    assert!(registry.peer_supports_protocol(&peer, "/my-protocol/1.0.0"));
    assert_eq!(registry.identified_shard_peer_count(&blockchain), 1);
}

#[test]
fn test_apply_peer_event_updates_latency_and_discovery() {
    let registry = PeerRegistry::new();
    let blockchain = BlockchainId::from(TEST_BLOCKCHAIN.to_string());
    let peer = PeerId::random();
    register_peer(&registry, peer);
    assert!(registry.is_peer_in_shard(&blockchain, &peer));

    registry.apply_peer_event(PeerEvent::RequestOutcome(RequestOutcome {
        peer_id: peer,
        protocol: PROTOCOL_NAME_BATCH_GET,
        outcome: RequestOutcomeKind::Success,
        elapsed: Duration::from_millis(25),
    }));
    assert_eq!(registry.get_average_latency(&peer), 25);

    registry.apply_peer_event(PeerEvent::KadLookup {
        target: peer,
        found: false,
    });
    assert!(!registry.should_attempt_discovery(&peer));
    assert!(registry.discovery_backoff(&peer).is_some());

    registry.apply_peer_event(PeerEvent::ConnectionEstablished { peer_id: peer });
    assert!(registry.should_attempt_discovery(&peer));
    assert!(registry.discovery_backoff(&peer).is_none());
}

#[test]
fn test_first_missing_shard_membership_none_when_peer_present() {
    let registry = PeerRegistry::new();
    let peer = PeerId::random();

    let chain_a = BlockchainId::from("test:chain:a".to_string());
    let chain_b = BlockchainId::from("test:chain:b".to_string());
    registry.set_shard_membership(&chain_a, &[peer]);
    registry.set_shard_membership(&chain_b, &[peer]);

    let missing = registry.first_missing_shard_membership(&peer, [&chain_a, &chain_b]);
    assert_eq!(missing, None);
}

#[test]
fn test_first_missing_shard_membership_returns_first_missing_chain() {
    let registry = PeerRegistry::new();
    let peer = PeerId::random();

    let chain_a = BlockchainId::from("test:chain:a".to_string());
    let chain_b = BlockchainId::from("test:chain:b".to_string());
    registry.set_shard_membership(&chain_a, &[peer]);

    let missing = registry.first_missing_shard_membership(&peer, [&chain_a, &chain_b]);
    assert_eq!(missing, Some(chain_b));
}
