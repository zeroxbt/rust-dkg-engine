use std::{collections::HashMap, sync::Arc, time::Duration};

use libp2p::{Multiaddr, PeerId};
use tokio::sync::broadcast;

use super::registry::PeerRegistry;
use crate::{
    managers::network::{
        PeerEvent, RequestOutcomeKind,
        protocols::{BatchGetProtocol, GetProtocol, ProtocolSpec},
    },
    types::BlockchainId,
};

pub(crate) struct PeerService {
    registry: Arc<PeerRegistry>,
}

impl PeerService {
    pub(crate) fn new() -> Self {
        Self {
            registry: Arc::new(PeerRegistry::new()),
        }
    }

    pub(crate) fn start(
        self: Arc<Self>,
        mut peer_event_rx: broadcast::Receiver<PeerEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn(async move {
            loop {
                match peer_event_rx.recv().await {
                    Ok(event) => self.handle_event(event),
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        })
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Identity methods
    // ─────────────────────────────────────────────────────────────────────────────

    pub(crate) fn peer_supports_protocol(&self, peer_id: &PeerId, protocol: &'static str) -> bool {
        self.registry.peer_supports_protocol(peer_id, protocol)
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Shard membership methods
    // ─────────────────────────────────────────────────────────────────────────────

    pub(crate) fn set_shard_membership(&self, blockchain_id: &BlockchainId, peer_ids: &[PeerId]) {
        self.registry.set_shard_membership(blockchain_id, peer_ids);
    }

    /// Count peers in a shard (for change detection optimization).
    pub(crate) fn shard_peer_count(&self, blockchain_id: &BlockchainId) -> usize {
        self.registry.shard_peer_count(blockchain_id)
    }

    /// Count shard peers that have been identified (received identify info).
    pub(crate) fn identified_shard_peer_count(&self, blockchain_id: &BlockchainId) -> usize {
        self.registry.identified_shard_peer_count(blockchain_id)
    }

    /// Get shard peers that support a protocol, sorted by latency (fastest first).
    /// Excludes the specified peer (typically self).
    pub(crate) fn select_shard_peers(
        &self,
        blockchain_id: &BlockchainId,
        protocol: &'static str,
        exclude_peer: Option<&PeerId>,
    ) -> Vec<PeerId> {
        self.registry
            .select_shard_peers(blockchain_id, protocol, exclude_peer)
    }

    /// Check if a peer is in a specific shard.
    pub(crate) fn is_peer_in_shard(&self, blockchain_id: &BlockchainId, peer_id: &PeerId) -> bool {
        self.registry.is_peer_in_shard(blockchain_id, peer_id)
    }

    /// Get all unique peer IDs across all shards.
    pub(crate) fn get_all_shard_peer_ids(&self) -> Vec<PeerId> {
        self.registry.get_all_shard_peer_ids()
    }

    /// Get all known peer addresses from identify info.
    pub(crate) fn get_all_addresses(&self) -> HashMap<PeerId, Vec<Multiaddr>> {
        self.registry.get_all_addresses()
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Performance methods
    // ─────────────────────────────────────────────────────────────────────────────

    pub(crate) fn get_average_latency(&self, peer_id: &PeerId) -> u64 {
        self.registry.get_average_latency(peer_id)
    }

    pub(crate) fn sort_by_latency(&self, peers: &mut [PeerId]) {
        self.registry.sort_by_latency(peers);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Discovery methods
    // ─────────────────────────────────────────────────────────────────────────────

    pub(crate) fn should_attempt_discovery(&self, peer_id: &PeerId) -> bool {
        self.registry.should_attempt_discovery(peer_id)
    }

    pub(crate) fn discovery_backoff(&self, peer_id: &PeerId) -> Option<Duration> {
        self.registry.get_discovery_backoff(peer_id)
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // Event handling
    // ─────────────────────────────────────────────────────────────────────────────

    fn handle_event(&self, event: PeerEvent) {
        match event {
            PeerEvent::IdentifyReceived { peer_id, info } => {
                self.registry.update_identify(peer_id, info);
            }
            PeerEvent::RequestOutcome(outcome) => match outcome.protocol {
                protocol if protocol == BatchGetProtocol::NAME => match outcome.outcome {
                    RequestOutcomeKind::Success => {
                        self.registry
                            .record_latency(outcome.peer_id, outcome.elapsed);
                    }
                    RequestOutcomeKind::ResponseError | RequestOutcomeKind::Failure => {
                        self.registry.record_request_failure(outcome.peer_id);
                    }
                },
                protocol if protocol == GetProtocol::NAME => {
                    if matches!(outcome.outcome, RequestOutcomeKind::Failure) {
                        self.registry.record_request_failure(outcome.peer_id);
                    }
                }
                _ => {}
            },
            PeerEvent::KadLookup { target, found } => {
                if found {
                    self.registry.record_discovery_success(target);
                } else {
                    self.registry.record_discovery_failure(target);
                }
            }
            PeerEvent::ConnectionEstablished { peer_id } => {
                self.registry.record_discovery_success(peer_id);
            }
        }
    }
}

impl Default for PeerService {
    fn default() -> Self {
        Self::new()
    }
}
