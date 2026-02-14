use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use dashmap::DashMap;
use libp2p::{Multiaddr, PeerId, StreamProtocol};

use crate::{managers::network::IdentifyInfo, types::BlockchainId};

#[derive(Clone, Debug)]
#[allow(dead_code)] // Used in diagnostics via `tracing` debug formatting.
pub(crate) struct PeerShardInfo {
    pub peer_id: PeerId,
    pub shard_membership: Vec<BlockchainId>,
    pub identified: bool,
}

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
    }

    pub(crate) fn peer_supports_protocol(&self, peer_id: &PeerId, protocol: &'static str) -> bool {
        let protocol = StreamProtocol::new(protocol);
        self.peers
            .get(peer_id)
            .and_then(|entry| entry.identify.as_ref().cloned())
            .map(|identify| identify.protocols.contains(&protocol))
            .unwrap_or(false)
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
    }

    /// Count peers in a shard (for change detection optimization).
    pub(crate) fn shard_peer_count(&self, blockchain_id: &BlockchainId) -> usize {
        self.peers
            .iter()
            .filter(|entry| entry.shard_membership.contains(blockchain_id))
            .count()
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

    /// Get shard peers that support a protocol, sorted by latency (fastest first).
    /// Excludes the local peer if provided.
    pub(crate) fn select_shard_peers(
        &self,
        blockchain_id: &BlockchainId,
        protocol: &'static str,
        exclude_peer: Option<&PeerId>,
    ) -> Vec<PeerId> {
        let protocol = StreamProtocol::new(protocol);

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
        peers
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

    /// Debug helper: snapshot the whole registry with shard membership info.
    /// Intended for diagnostics in production logs.
    pub(crate) fn dump_peers_with_shards(&self) -> Vec<PeerShardInfo> {
        let mut out: Vec<PeerShardInfo> = self
            .peers
            .iter()
            .map(|entry| {
                let mut shard_membership: Vec<BlockchainId> =
                    entry.shard_membership.iter().cloned().collect();
                shard_membership.sort_by(|a, b| a.as_str().cmp(b.as_str()));

                PeerShardInfo {
                    peer_id: *entry.key(),
                    shard_membership,
                    identified: entry.identify.is_some(),
                }
            })
            .collect();

        // Stable ordering helps comparing dumps across nodes/timestamps.
        out.sort_by(|a, b| a.peer_id.to_base58().cmp(&b.peer_id.to_base58()));
        out
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
    }

    /// Record a successful connection - resets failure tracking.
    pub(crate) fn record_discovery_success(&self, peer_id: PeerId) {
        if let Some(mut peer) = self.peers.get_mut(&peer_id) {
            peer.discovery_failure_count = 0;
            peer.last_discovery_attempt = None;
        }
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
}
