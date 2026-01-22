use std::time::{Duration, Instant};

use dashmap::DashMap;
use libp2p::PeerId;

/// Tracks failed peer discovery attempts with exponential backoff.
///
/// When a DHT lookup fails to find a peer, we record the failure and apply
/// exponential backoff to avoid repeatedly querying for offline peers.
pub struct PeerDiscoveryTracker {
    /// Maps peer ID to (failure_count, last_attempt_time)
    failed_peers: DashMap<PeerId, (u32, Instant)>,
    /// Base delay (applied after first failure)
    base_delay: Duration,
    /// Maximum delay (cap for exponential backoff)
    max_delay: Duration,
}

impl PeerDiscoveryTracker {
    pub fn new() -> Self {
        Self {
            failed_peers: DashMap::new(),
            base_delay: Duration::from_secs(60),  // 1 minute base delay
            max_delay: Duration::from_secs(960),  // 16 minutes max delay
        }
    }

    /// Check if we should attempt to discover this peer.
    /// Returns true if enough time has passed since the last failed attempt.
    pub fn should_attempt(&self, peer_id: &PeerId) -> bool {
        match self.failed_peers.get(peer_id) {
            Some(entry) => {
                let (failure_count, last_attempt) = *entry;
                let backoff = self.calculate_backoff(failure_count);
                last_attempt.elapsed() >= backoff
            }
            None => true, // Never failed, should attempt
        }
    }

    /// Record a failed discovery attempt for a peer.
    pub fn record_failure(&self, peer_id: PeerId) {
        self.failed_peers
            .entry(peer_id)
            .and_modify(|(count, last_attempt)| {
                *count = count.saturating_add(1);
                *last_attempt = Instant::now();
            })
            .or_insert((1, Instant::now()));
    }

    /// Record a successful connection - removes the peer from failed tracking.
    pub fn record_success(&self, peer_id: &PeerId) {
        self.failed_peers.remove(peer_id);
    }

    /// Calculate backoff delay based on failure count.
    /// Uses exponential backoff: base_delay * 2^(failures-1), capped at max_delay.
    fn calculate_backoff(&self, failure_count: u32) -> Duration {
        if failure_count == 0 {
            return Duration::ZERO;
        }
        let multiplier = 1u32 << (failure_count - 1).min(10); // 2^(n-1), cap exponent at 10
        self.base_delay.saturating_mul(multiplier).min(self.max_delay)
    }

    /// Get the current backoff delay for a peer (for logging).
    pub fn get_backoff(&self, peer_id: &PeerId) -> Option<Duration> {
        self.failed_peers
            .get(peer_id)
            .map(|entry| self.calculate_backoff(entry.0))
    }
}

impl Default for PeerDiscoveryTracker {
    fn default() -> Self {
        Self::new()
    }
}
