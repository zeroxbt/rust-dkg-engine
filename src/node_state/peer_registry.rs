use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use dashmap::DashMap;
use dkg_domain::BlockchainId;
use dkg_network::{
    IdentifyInfo, Multiaddr, PROTOCOL_NAME_BATCH_GET, PROTOCOL_NAME_GET, PeerEvent, PeerId,
    RequestOutcomeKind, StreamProtocol,
};
use dkg_observability as observability;

// Performance tracking constants
const HISTORY_SIZE: usize = 20;
const FAILURE_LATENCY_MS: u64 = 15_000;
const DEFAULT_LATENCY_MS: u64 = 5_000;

// Discovery backoff constants
const DISCOVERY_BASE_DELAY: Duration = Duration::from_secs(60);
const DISCOVERY_MAX_DELAY: Duration = Duration::from_secs(960);

#[derive(Clone, Debug)]
pub(crate) struct IdentifyRecord {
    pub protocols: Vec<StreamProtocol>,
    pub listen_addrs: Vec<Multiaddr>,
}

/// Rolling latency stats stored per peer.
#[derive(Clone, Debug)]
pub(crate) struct PerformanceStats {
    /// Circular buffer of latencies in milliseconds
    latencies: [u64; HISTORY_SIZE],
    /// Next write position in the ring buffer
    index: usize,
    /// Number of recorded entries (0 to HISTORY_SIZE)
    count: usize,
}

impl Default for PerformanceStats {
    fn default() -> Self {
        Self {
            latencies: [0; HISTORY_SIZE],
            index: 0,
            count: 0,
        }
    }
}

impl PerformanceStats {
    fn record(&mut self, latency_ms: u64) {
        self.latencies[self.index] = latency_ms;
        self.index = (self.index + 1) % HISTORY_SIZE;
        if self.count < HISTORY_SIZE {
            self.count += 1;
        }
    }

    fn average_latency(&self) -> u64 {
        if self.count == 0 {
            return DEFAULT_LATENCY_MS;
        }
        let sum: u64 = self.latencies[..self.count].iter().sum();
        sum / self.count as u64
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PeerRecord {
    pub identify: Option<IdentifyRecord>,
    pub shard_membership: HashSet<BlockchainId>,
    performance: PerformanceStats,
    last_failure_at: Option<Instant>,
    /// Number of consecutive discovery failures (reset on success)
    discovery_failure_count: u32,
    /// Timestamp of last discovery attempt (for backoff calculation)
    last_discovery_attempt: Option<Instant>,
}

pub(crate) struct PeerRegistry {
    peers: DashMap<PeerId, PeerRecord>,
}

impl PeerRegistry {
    pub(crate) fn new() -> Self {
        Self {
            peers: DashMap::new(),
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Identity methods
    // ─────────────────────────────────────────────────────────────────────────────

    pub(crate) fn update_identify(&self, peer_id: PeerId, info: IdentifyInfo) {
        let identify = IdentifyRecord {
            protocols: info.protocols,
            listen_addrs: info.listen_addrs,
        };

        self.peers
            .entry(peer_id)
            .and_modify(|peer| {
                peer.identify = Some(identify.clone());
            })
            .or_insert_with(|| PeerRecord {
                identify: Some(identify),
                ..Default::default()
            });

        let shard_memberships = self
            .peers
            .get(&peer_id)
            .map(|peer| peer.shard_membership.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        for blockchain_id in &shard_memberships {
            self.record_population_metrics(blockchain_id);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Shard membership methods
    // ─────────────────────────────────────────────────────────────────────────────

    pub(crate) fn set_shard_membership(&self, blockchain_id: &BlockchainId, peer_ids: &[PeerId]) {
        let new_set: HashSet<PeerId> = peer_ids.iter().copied().collect();

        for peer_id in new_set.iter() {
            self.peers
                .entry(*peer_id)
                .and_modify(|peer| {
                    peer.shard_membership.insert(blockchain_id.clone());
                })
                .or_insert_with(|| {
                    let mut record = PeerRecord::default();
                    record.shard_membership.insert(blockchain_id.clone());
                    record
                });
        }

        for mut entry in self.peers.iter_mut() {
            if entry.shard_membership.contains(blockchain_id) && !new_set.contains(entry.key()) {
                entry.shard_membership.remove(blockchain_id);
            }
        }

        self.record_population_metrics(blockchain_id);
    }

    /// Count peers in a shard (for change detection optimization).
    pub(crate) fn shard_peer_count(&self, blockchain_id: &BlockchainId) -> usize {
        self.peers
            .iter()
            .filter(|entry| entry.shard_membership.contains(blockchain_id))
            .count()
    }

    /// Count all known peers in the registry.
    pub(crate) fn known_peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Count shard peers that have been identified (received identify info).
    pub(crate) fn identified_shard_peer_count(&self, blockchain_id: &BlockchainId) -> usize {
        self.peers
            .iter()
            .filter(|entry| {
                entry.shard_membership.contains(blockchain_id) && entry.identify.is_some()
            })
            .count()
    }

    /// Count shard peers that support a specific protocol.
    pub(crate) fn protocol_capable_shard_peer_count(
        &self,
        blockchain_id: &BlockchainId,
        protocol: &'static str,
    ) -> usize {
        let Some(protocol) = Self::parse_stream_protocol(protocol) else {
            return 0;
        };
        self.peers
            .iter()
            .filter(|entry| {
                entry.shard_membership.contains(blockchain_id)
                    && entry
                        .identify
                        .as_ref()
                        .map(|id| id.protocols.contains(&protocol))
                        .unwrap_or(false)
            })
            .count()
    }

    /// Get shard peers that support a protocol, sorted by latency (fastest first).
    /// Excludes the local peer if provided.
    pub(crate) fn select_shard_peers(
        &self,
        blockchain_id: &BlockchainId,
        protocol: &'static str,
        exclude_peer: Option<&PeerId>,
    ) -> Vec<PeerId> {
        let protocol_label = protocol;
        let Some(protocol) = Self::parse_stream_protocol(protocol) else {
            return Vec::new();
        };
        let shard_peers = self.shard_peer_count(blockchain_id);

        let mut peers: Vec<PeerId> = self
            .peers
            .iter()
            .filter(|entry| {
                // Must be in shard
                if !entry.shard_membership.contains(blockchain_id) {
                    return false;
                }
                // Exclude self if specified
                if let Some(exclude) = exclude_peer
                    && entry.key() == exclude
                {
                    return false;
                }
                // Must support protocol (if we have identify info)
                entry
                    .identify
                    .as_ref()
                    .map(|id| id.protocols.contains(&protocol))
                    .unwrap_or(false)
            })
            .map(|entry| *entry.key())
            .collect();

        // Sort by latency
        self.sort_by_latency(&mut peers);
        observability::record_peer_registry_selection(
            blockchain_id.as_str(),
            protocol_label,
            shard_peers,
            peers.len(),
        );
        peers
    }

    fn parse_stream_protocol(protocol: &'static str) -> Option<StreamProtocol> {
        if !protocol.starts_with('/') {
            tracing::warn!(
                protocol,
                "Invalid stream protocol identifier; expected leading '/'"
            );
            return None;
        }
        Some(StreamProtocol::new(protocol))
    }

    /// Check if a peer is in a specific shard.
    pub(crate) fn is_peer_in_shard(&self, blockchain_id: &BlockchainId, peer_id: &PeerId) -> bool {
        self.peers
            .get(peer_id)
            .map(|entry| entry.shard_membership.contains(blockchain_id))
            .unwrap_or(false)
    }

    /// Get all unique peer IDs across all shards.
    pub(crate) fn get_all_shard_peer_ids(&self) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter(|entry| !entry.shard_membership.is_empty())
            .map(|entry| *entry.key())
            .collect()
    }

    /// Get all known peer addresses from identify info.
    /// Returns peers that have identify info with non-empty listen addresses.
    pub(crate) fn get_all_addresses(&self) -> HashMap<PeerId, Vec<Multiaddr>> {
        self.peers
            .iter()
            .filter_map(|entry| {
                entry.identify.as_ref().and_then(|id| {
                    if id.listen_addrs.is_empty() {
                        None
                    } else {
                        Some((*entry.key(), id.listen_addrs.clone()))
                    }
                })
            })
            .collect()
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Performance tracking methods
    // ─────────────────────────────────────────────────────────────────────────────

    /// Record a successful request with its latency.
    pub(crate) fn record_latency(&self, peer_id: PeerId, latency: Duration) {
        if let Some(mut peer) = self.peers.get_mut(&peer_id) {
            peer.performance.record(latency.as_millis() as u64);
        }
    }

    /// Record a failed request (timeout, dial failure, etc.).
    /// Failures are recorded as high latency to naturally sort failing peers last.
    pub(crate) fn record_request_failure(&self, peer_id: PeerId) {
        if let Some(mut peer) = self.peers.get_mut(&peer_id) {
            peer.performance.record(FAILURE_LATENCY_MS);
            peer.last_failure_at = Some(Instant::now());
        }
    }

    /// Get the average latency for a peer.
    /// Returns DEFAULT_LATENCY_MS for unknown peers.
    pub(crate) fn get_average_latency(&self, peer_id: &PeerId) -> u64 {
        self.peers
            .get(peer_id)
            .map(|entry| entry.performance.average_latency())
            .unwrap_or(DEFAULT_LATENCY_MS)
    }

    /// Sort peers by their average latency (fastest first).
    /// Unknown peers get DEFAULT_LATENCY_MS and are placed in the middle.
    pub(crate) fn sort_by_latency(&self, peers: &mut [PeerId]) {
        peers.sort_by(|a, b| {
            let latency_a = self.get_average_latency(a);
            let latency_b = self.get_average_latency(b);
            latency_a.cmp(&latency_b)
        });
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Discovery tracking methods (with exponential backoff)
    // ─────────────────────────────────────────────────────────────────────────────

    /// Check if we should attempt to discover this peer.
    /// Returns true if enough time has passed since the last failed attempt.
    pub(crate) fn should_attempt_discovery(&self, peer_id: &PeerId) -> bool {
        match self.peers.get(peer_id) {
            Some(entry) => {
                if entry.discovery_failure_count == 0 {
                    return true;
                }
                match entry.last_discovery_attempt {
                    Some(last_attempt) => {
                        let backoff = self.calculate_backoff(entry.discovery_failure_count);
                        last_attempt.elapsed() >= backoff
                    }
                    None => true,
                }
            }
            None => true,
        }
    }

    /// Record a failed discovery attempt for a peer.
    pub(crate) fn record_discovery_failure(&self, peer_id: PeerId) {
        if let Some(mut peer) = self.peers.get_mut(&peer_id) {
            peer.discovery_failure_count = peer.discovery_failure_count.saturating_add(1);
            peer.last_discovery_attempt = Some(Instant::now());
        }
        self.record_discovery_backoff_metric();
    }

    /// Record a successful connection - resets failure tracking.
    pub(crate) fn record_discovery_success(&self, peer_id: PeerId) {
        if let Some(mut peer) = self.peers.get_mut(&peer_id) {
            peer.discovery_failure_count = 0;
            peer.last_discovery_attempt = None;
        }
        self.record_discovery_backoff_metric();
    }

    /// Get the current backoff delay for a peer (for logging).
    pub(crate) fn get_discovery_backoff(&self, peer_id: &PeerId) -> Option<Duration> {
        self.peers.get(peer_id).and_then(|entry| {
            if entry.discovery_failure_count > 0 {
                Some(self.calculate_backoff(entry.discovery_failure_count))
            } else {
                None
            }
        })
    }

    pub(crate) fn discovery_backoff(&self, peer_id: &PeerId) -> Option<Duration> {
        self.get_discovery_backoff(peer_id)
    }

    /// Count peers currently under discovery backoff.
    pub(crate) fn discovery_backoff_active_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|entry| entry.discovery_failure_count > 0)
            .count()
    }

    pub(crate) fn apply_peer_event(&self, event: PeerEvent) {
        match event {
            PeerEvent::IdentifyReceived { peer_id, info } => {
                self.update_identify(peer_id, info);
            }
            PeerEvent::RequestOutcome(outcome) => {
                let outcome_label = match &outcome.outcome {
                    RequestOutcomeKind::Success => "success",
                    RequestOutcomeKind::ResponseError => "response_error",
                    RequestOutcomeKind::Failure => "failure",
                };
                observability::record_peer_registry_request_outcome(
                    outcome.protocol,
                    outcome_label,
                    outcome.elapsed,
                );

                match outcome.protocol {
                    protocol if protocol == PROTOCOL_NAME_BATCH_GET => match outcome.outcome {
                        RequestOutcomeKind::Success => {
                            self.record_latency(outcome.peer_id, outcome.elapsed);
                        }
                        RequestOutcomeKind::ResponseError => {
                            tracing::debug!(
                                peer_id = %outcome.peer_id,
                                protocol = outcome.protocol,
                                elapsed_ms = outcome.elapsed.as_millis() as u64,
                                "Peer request failed with response error"
                            );
                            self.record_request_failure(outcome.peer_id);
                        }
                        RequestOutcomeKind::Failure => {
                            tracing::warn!(
                                peer_id = %outcome.peer_id,
                                protocol = outcome.protocol,
                                elapsed_ms = outcome.elapsed.as_millis() as u64,
                                "Peer request failed (timeout/dial/network)"
                            );
                            self.record_request_failure(outcome.peer_id);
                        }
                    },
                    protocol if protocol == PROTOCOL_NAME_GET => {
                        if matches!(outcome.outcome, RequestOutcomeKind::Failure) {
                            tracing::warn!(
                                peer_id = %outcome.peer_id,
                                protocol = outcome.protocol,
                                elapsed_ms = outcome.elapsed.as_millis() as u64,
                                "GET request to peer failed (timeout/dial/network)"
                            );
                            self.record_request_failure(outcome.peer_id);
                        }
                    }
                    _ => {}
                }
            }
            PeerEvent::KadLookup { target, found } => {
                if found {
                    self.record_discovery_success(target);
                } else {
                    self.record_discovery_failure(target);
                    if let Some(backoff) = self.get_discovery_backoff(&target) {
                        tracing::debug!(
                            peer_id = %target,
                            backoff_secs = backoff.as_secs(),
                            "Peer discovery failed, applying backoff"
                        );
                    }
                }
            }
            PeerEvent::ConnectionEstablished { peer_id } => {
                self.record_discovery_success(peer_id);
                tracing::debug!(peer_id = %peer_id, "Peer connection established");
            }
        }
    }

    fn record_population_metrics(&self, blockchain_id: &BlockchainId) {
        observability::record_peer_registry_population(
            blockchain_id.as_str(),
            self.known_peer_count(),
            self.shard_peer_count(blockchain_id),
            self.identified_shard_peer_count(blockchain_id),
        );
    }

    fn record_discovery_backoff_metric(&self) {
        let active = self.discovery_backoff_active_count();
        observability::record_peer_registry_discovery_backoff(active);
    }

    /// Calculate backoff delay based on failure count.
    /// Uses exponential backoff: base_delay * 2^(failures-1), capped at max_delay.
    fn calculate_backoff(&self, failure_count: u32) -> Duration {
        if failure_count == 0 {
            return Duration::ZERO;
        }
        let multiplier = 1u32 << (failure_count - 1).min(10);
        DISCOVERY_BASE_DELAY
            .saturating_mul(multiplier)
            .min(DISCOVERY_MAX_DELAY)
    }
}

impl Default for PeerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use dkg_network::{PROTOCOL_NAME_BATCH_GET, PeerEvent, RequestOutcome, RequestOutcomeKind};

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

        let selected = registry.select_shard_peers(&blockchain, "/my-protocol/1.0.0", None);
        assert!(selected.contains(&peer));
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
    fn invalid_protocol_string_does_not_panic() {
        let registry = PeerRegistry::new();
        let blockchain = BlockchainId::from(TEST_BLOCKCHAIN.to_string());
        let peer = PeerId::random();
        register_peer(&registry, peer);

        assert_eq!(
            registry.protocol_capable_shard_peer_count(&blockchain, "BatchGet"),
            0
        );
        assert!(
            registry
                .select_shard_peers(&blockchain, "BatchGet", None)
                .is_empty()
        );
    }
}
