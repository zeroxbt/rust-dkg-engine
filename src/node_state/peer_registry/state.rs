use std::{collections::HashSet, time::Instant};

use dkg_domain::BlockchainId;
use dkg_network::{Multiaddr, StreamProtocol};

use super::config::{DEFAULT_LATENCY_MS, HISTORY_SIZE};

#[derive(Clone, Debug)]
pub(super) struct IdentifyRecord {
    pub(super) protocols: Vec<StreamProtocol>,
    pub(super) listen_addrs: Vec<Multiaddr>,
}

/// Rolling latency stats stored per peer.
#[derive(Clone, Debug)]
pub(super) struct PerformanceStats {
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
    pub(super) fn record(&mut self, latency_ms: u64) {
        self.latencies[self.index] = latency_ms;
        self.index = (self.index + 1) % HISTORY_SIZE;
        if self.count < HISTORY_SIZE {
            self.count += 1;
        }
    }

    pub(super) fn average_latency(&self) -> u64 {
        if self.count == 0 {
            return DEFAULT_LATENCY_MS;
        }
        let sum: u64 = self.latencies[..self.count].iter().sum();
        sum / self.count as u64
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct PeerRecord {
    pub(super) identify: Option<IdentifyRecord>,
    pub(super) shard_membership: HashSet<BlockchainId>,
    pub(super) performance: PerformanceStats,
    pub(super) last_failure_at: Option<Instant>,
    /// Number of consecutive discovery failures (reset on success)
    pub(super) discovery_failure_count: u32,
    /// Timestamp of last discovery attempt (for backoff calculation)
    pub(super) last_discovery_attempt: Option<Instant>,
}
