use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use dkg_domain::BlockchainId;
use dkg_network::{
    IdentifyInfo, PROTOCOL_NAME_BATCH_GET, PROTOCOL_NAME_GET, PeerEvent, PeerId, RequestOutcomeKind,
};

use super::{
    PeerRegistry,
    config::{DISCOVERY_BASE_DELAY, DISCOVERY_MAX_DELAY, FAILURE_LATENCY_MS},
    state::{IdentifyRecord, PeerRecord},
};

impl PeerRegistry {
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

    // ─────────────────────────────────────────────────────────────────────────────
    // Shard membership methods
    // ─────────────────────────────────────────────────────────────────────────────

    pub(crate) fn set_shard_membership(&self, blockchain_id: &BlockchainId, peer_ids: &[PeerId]) {
        let new_set: HashSet<PeerId> = peer_ids.iter().copied().collect();

        for peer_id in &new_set {
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

    // ─────────────────────────────────────────────────────────────────────────────
    // Discovery tracking methods (with exponential backoff)
    // ─────────────────────────────────────────────────────────────────────────────

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

    pub(crate) fn apply_peer_event(&self, event: PeerEvent) {
        match event {
            PeerEvent::IdentifyReceived { peer_id, info } => {
                self.update_identify(peer_id, info);
            }
            PeerEvent::RequestOutcome(outcome) => match outcome.protocol {
                protocol if protocol == PROTOCOL_NAME_BATCH_GET => match outcome.outcome {
                    RequestOutcomeKind::Success => {
                        self.record_latency(outcome.peer_id, outcome.elapsed);
                    }
                    RequestOutcomeKind::ResponseError | RequestOutcomeKind::Failure => {
                        self.record_request_failure(outcome.peer_id);
                    }
                },
                protocol if protocol == PROTOCOL_NAME_GET => {
                    if matches!(outcome.outcome, RequestOutcomeKind::Failure) {
                        self.record_request_failure(outcome.peer_id);
                    }
                }
                _ => {}
            },
            PeerEvent::KadLookup { target, found } => {
                if found {
                    self.record_discovery_success(target);
                } else {
                    self.record_discovery_failure(target);
                }
            }
            PeerEvent::ConnectionEstablished { peer_id } => {
                self.record_discovery_success(peer_id);
            }
        }
    }

    /// Calculate backoff delay based on failure count.
    /// Uses exponential backoff: base_delay * 2^(failures-1), capped at max_delay.
    pub(super) fn calculate_backoff(&self, failure_count: u32) -> Duration {
        if failure_count == 0 {
            return Duration::ZERO;
        }
        let multiplier = 1u32 << (failure_count - 1).min(10);
        DISCOVERY_BASE_DELAY
            .saturating_mul(multiplier)
            .min(DISCOVERY_MAX_DELAY)
    }
}
