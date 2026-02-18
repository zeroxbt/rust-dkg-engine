use std::{collections::HashMap, sync::Arc, time::Duration};

use dkg_domain::BlockchainId;
use dkg_network::{
    Multiaddr, PROTOCOL_NAME_BATCH_GET, PROTOCOL_NAME_GET, PeerEvent, PeerId, RequestOutcomeKind,
};
use tokio::sync::broadcast;

use crate::state::PeerRegistry;

pub(crate) struct PeerDirectory {
    registry: Arc<PeerRegistry>,
}

impl PeerDirectory {
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
                protocol if protocol == PROTOCOL_NAME_BATCH_GET => match outcome.outcome {
                    RequestOutcomeKind::Success => {
                        self.registry
                            .record_latency(outcome.peer_id, outcome.elapsed);
                    }
                    RequestOutcomeKind::ResponseError | RequestOutcomeKind::Failure => {
                        self.registry.record_request_failure(outcome.peer_id);
                    }
                },
                protocol if protocol == PROTOCOL_NAME_GET => {
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

impl Default for PeerDirectory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use dkg_network::{
        IdentifyInfo, PROTOCOL_NAME_BATCH_GET, PeerEvent, RequestOutcome,
        RequestOutcomeKind, StreamProtocol,
        STREAM_PROTOCOL_GET,
    };
    use tokio::sync::broadcast;

    use super::*;

    async fn wait_until<F>(mut condition: F)
    where
        F: FnMut() -> bool,
    {
        for _ in 0..50 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not met within timeout");
    }

    #[tokio::test]
    async fn start_loop_updates_protocol_support_and_addresses() {
        let peer_directory = Arc::new(PeerDirectory::new());
        let blockchain_id = BlockchainId::from("hardhat1:31337".to_string());
        let peer = PeerId::random();

        peer_directory.set_shard_membership(&blockchain_id, &[peer]);
        assert!(peer_directory.is_peer_in_shard(&blockchain_id, &peer));

        let (peer_tx, peer_rx) = broadcast::channel(16);
        let task = Arc::clone(&peer_directory).start(peer_rx);

        let identify = IdentifyInfo {
            protocols: vec![StreamProtocol::new(STREAM_PROTOCOL_GET)],
            listen_addrs: vec!["/ip4/127.0.0.1/tcp/4100".parse().expect("valid addr")],
        };
        peer_tx
            .send(PeerEvent::IdentifyReceived {
                peer_id: peer,
                info: identify,
            })
            .expect("send identify");

        wait_until(|| peer_directory.peer_supports_protocol(&peer, STREAM_PROTOCOL_GET)).await;

        let addresses = peer_directory.get_all_addresses();
        assert!(addresses.contains_key(&peer));

        drop(peer_tx);
        task.await.expect("peer loop task");
    }

    #[tokio::test]
    async fn event_loop_tracks_latency_and_discovery_backoff() {
        let peer_directory = Arc::new(PeerDirectory::new());
        let blockchain_id = BlockchainId::from("hardhat1:31337".to_string());
        let fast_peer = PeerId::random();
        let slow_peer = PeerId::random();

        peer_directory.set_shard_membership(&blockchain_id, &[fast_peer, slow_peer]);

        let (peer_tx, peer_rx) = broadcast::channel(16);
        let task = Arc::clone(&peer_directory).start(peer_rx);

        peer_tx
            .send(PeerEvent::RequestOutcome(RequestOutcome {
                peer_id: fast_peer,
                protocol: PROTOCOL_NAME_BATCH_GET,
                outcome: RequestOutcomeKind::Success,
                elapsed: Duration::from_millis(10),
            }))
            .expect("send fast outcome");

        peer_tx
            .send(PeerEvent::RequestOutcome(RequestOutcome {
                peer_id: slow_peer,
                protocol: PROTOCOL_NAME_BATCH_GET,
                outcome: RequestOutcomeKind::Failure,
                elapsed: Duration::from_millis(5),
            }))
            .expect("send slow outcome");

        wait_until(|| peer_directory.get_average_latency(&fast_peer) < peer_directory.get_average_latency(&slow_peer)).await;

        let mut peers = vec![slow_peer, fast_peer];
        peer_directory.sort_by_latency(&mut peers);
        assert_eq!(peers[0], fast_peer);

        peer_tx
            .send(PeerEvent::KadLookup {
                target: fast_peer,
                found: false,
            })
            .expect("send failed lookup");

        wait_until(|| !peer_directory.should_attempt_discovery(&fast_peer)).await;
        assert!(peer_directory.discovery_backoff(&fast_peer).is_some());

        peer_tx
            .send(PeerEvent::ConnectionEstablished { peer_id: fast_peer })
            .expect("send connection established");
        wait_until(|| peer_directory.should_attempt_discovery(&fast_peer)).await;
        assert!(peer_directory.discovery_backoff(&fast_peer).is_none());

        drop(peer_tx);
        task.await.expect("peer loop task");
    }
}
