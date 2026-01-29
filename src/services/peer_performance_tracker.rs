use std::time::Duration;

use dashmap::DashMap;
use libp2p::PeerId;

/// Number of recent requests to track per peer
const HISTORY_SIZE: usize = 20;

/// Latency value used for failed requests (15 seconds - worse than 10s timeout)
const FAILURE_LATENCY_MS: u64 = 15_000;

/// Default latency for unknown peers (5 seconds - neutral middle value)
const DEFAULT_LATENCY_MS: u64 = 5_000;

/// Tracks peer request performance using a rolling window of recent response latencies.
///
/// Used by sync and get operations to prefer peers that respond quickly.
/// Failures (timeouts, dial failures) are recorded as high latency values
/// to naturally sort failing peers to the end.
///
/// Uses a ring buffer to track the last N requests per peer, ensuring scores
/// reflect recent performance rather than historical averages.
pub(crate) struct PeerPerformanceTracker {
    /// Maps peer ID to performance stats
    stats: DashMap<PeerId, PeerStats>,
}

/// Ring buffer tracking recent request latencies
struct PeerStats {
    /// Circular buffer of latencies in milliseconds
    latencies: [u64; HISTORY_SIZE],
    /// Next write position in the ring buffer
    index: usize,
    /// Number of recorded entries (0 to HISTORY_SIZE)
    count: usize,
}

impl PeerStats {
    fn new() -> Self {
        Self {
            latencies: [0; HISTORY_SIZE],
            index: 0,
            count: 0,
        }
    }

    /// Record a request latency in milliseconds
    fn record(&mut self, latency_ms: u64) {
        self.latencies[self.index] = latency_ms;
        self.index = (self.index + 1) % HISTORY_SIZE;
        if self.count < HISTORY_SIZE {
            self.count += 1;
        }
    }

    /// Calculate average latency from recorded history.
    /// Returns DEFAULT_LATENCY_MS for peers with no history (neutral score).
    fn average_latency(&self) -> u64 {
        if self.count == 0 {
            return DEFAULT_LATENCY_MS;
        }
        let sum: u64 = self.latencies[..self.count].iter().sum();
        sum / self.count as u64
    }
}

impl PeerPerformanceTracker {
    pub(crate) fn new() -> Self {
        Self {
            stats: DashMap::new(),
        }
    }

    /// Record a successful request with its latency.
    pub(crate) fn record_latency(&self, peer_id: &PeerId, latency: Duration) {
        let latency_ms = latency.as_millis() as u64;
        self.stats
            .entry(*peer_id)
            .and_modify(|stats| stats.record(latency_ms))
            .or_insert_with(|| {
                let mut stats = PeerStats::new();
                stats.record(latency_ms);
                stats
            });
    }

    /// Record a failed request (timeout, dial failure, etc.).
    /// Failures are recorded as high latency to sort them last.
    pub(crate) fn record_failure(&self, peer_id: &PeerId) {
        self.stats
            .entry(*peer_id)
            .and_modify(|stats| stats.record(FAILURE_LATENCY_MS))
            .or_insert_with(|| {
                let mut stats = PeerStats::new();
                stats.record(FAILURE_LATENCY_MS);
                stats
            });
    }

    /// Get the average latency for a single peer (for logging/debugging).
    /// Returns DEFAULT_LATENCY_MS for unknown peers.
    pub(crate) fn get_average_latency(&self, peer_id: &PeerId) -> u64 {
        self.stats
            .get(peer_id)
            .map(|stats| stats.average_latency())
            .unwrap_or(DEFAULT_LATENCY_MS)
    }

    /// Sort peers by their average latency (fastest first).
    /// Unknown peers get DEFAULT_LATENCY_MS and are placed in the middle.
    pub(crate) fn sort_by_latency(&self, peers: &mut [PeerId]) {
        peers.sort_by(|a, b| {
            let latency_a = self.get_average_latency(a);
            let latency_b = self.get_average_latency(b);
            // Sort ascending (lower latency first)
            latency_a.cmp(&latency_b)
        });
    }
}

impl Default for PeerPerformanceTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_peer_gets_default_latency() {
        let tracker = PeerPerformanceTracker::new();
        let peer = PeerId::random();
        assert_eq!(tracker.get_average_latency(&peer), DEFAULT_LATENCY_MS);
    }

    #[test]
    fn test_record_latency() {
        let tracker = PeerPerformanceTracker::new();
        let peer = PeerId::random();

        tracker.record_latency(&peer, Duration::from_millis(100));
        assert_eq!(tracker.get_average_latency(&peer), 100);

        tracker.record_latency(&peer, Duration::from_millis(200));
        assert_eq!(tracker.get_average_latency(&peer), 150); // (100 + 200) / 2
    }

    #[test]
    fn test_failure_records_high_latency() {
        let tracker = PeerPerformanceTracker::new();
        let peer = PeerId::random();

        tracker.record_failure(&peer);
        assert_eq!(tracker.get_average_latency(&peer), FAILURE_LATENCY_MS);
    }

    #[test]
    fn test_mixed_success_and_failure() {
        let tracker = PeerPerformanceTracker::new();
        let peer = PeerId::random();

        // 1 success at 1000ms, 1 failure at 15000ms
        tracker.record_latency(&peer, Duration::from_millis(1000));
        tracker.record_failure(&peer);
        // Average: (1000 + 15000) / 2 = 8000
        assert_eq!(tracker.get_average_latency(&peer), 8000);
    }

    #[test]
    fn test_ring_buffer_overwrites_old_entries() {
        let tracker = PeerPerformanceTracker::new();
        let peer = PeerId::random();

        // Fill buffer with 100ms latencies (HISTORY_SIZE = 20)
        for _ in 0..20 {
            tracker.record_latency(&peer, Duration::from_millis(100));
        }
        assert_eq!(tracker.get_average_latency(&peer), 100);

        // Now add 10 entries at 200ms - should overwrite 10 of the 100ms entries
        for _ in 0..10 {
            tracker.record_latency(&peer, Duration::from_millis(200));
        }
        // 10 entries at 100ms + 10 entries at 200ms = average 150ms
        assert_eq!(tracker.get_average_latency(&peer), 150);

        // Add 10 more at 200ms - should overwrite remaining 100ms entries
        for _ in 0..10 {
            tracker.record_latency(&peer, Duration::from_millis(200));
        }
        // All 20 are now 200ms
        assert_eq!(tracker.get_average_latency(&peer), 200);
    }

    #[test]
    fn test_sort_by_latency() {
        let tracker = PeerPerformanceTracker::new();

        let fast_peer = PeerId::random();
        let slow_peer = PeerId::random();
        let failing_peer = PeerId::random();
        let unknown_peer = PeerId::random();

        // Fast peer: 500ms average
        tracker.record_latency(&fast_peer, Duration::from_millis(500));
        tracker.record_latency(&fast_peer, Duration::from_millis(500));

        // Slow peer: 8000ms average
        tracker.record_latency(&slow_peer, Duration::from_millis(8000));
        tracker.record_latency(&slow_peer, Duration::from_millis(8000));

        // Failing peer: 15000ms (failure latency)
        tracker.record_failure(&failing_peer);
        tracker.record_failure(&failing_peer);

        // Unknown peer: no history (5000ms default)

        let mut peers = vec![failing_peer, unknown_peer, slow_peer, fast_peer];
        tracker.sort_by_latency(&mut peers);

        assert_eq!(peers[0], fast_peer); // 500ms
        assert_eq!(peers[1], unknown_peer); // 5000ms (default)
        assert_eq!(peers[2], slow_peer); // 8000ms
        assert_eq!(peers[3], failing_peer); // 15000ms
    }
}
