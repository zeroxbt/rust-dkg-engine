use std::time::Instant;

use dashmap::DashMap;
use libp2p::{Multiaddr, PeerId, StreamProtocol, identify};

/// Cached peer metadata derived from identify responses.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct PeerInfo {
    pub protocols: Vec<StreamProtocol>,
    pub listen_addrs: Vec<Multiaddr>,
    pub agent_version: String,
    pub protocol_version: String,
    pub observed_addr: Multiaddr,
    pub last_seen: Instant,
}

/// In-memory cache of peer metadata.
pub(crate) struct PeerStore {
    peers: DashMap<PeerId, PeerInfo>,
}

impl PeerStore {
    pub(crate) fn new() -> Self {
        Self {
            peers: DashMap::new(),
        }
    }

    pub(crate) fn record_identify(&self, peer_id: PeerId, info: &identify::Info) {
        let entry = PeerInfo {
            protocols: info.protocols.clone(),
            listen_addrs: info.listen_addrs.clone(),
            agent_version: info.agent_version.clone(),
            protocol_version: info.protocol_version.clone(),
            observed_addr: info.observed_addr.clone(),
            last_seen: Instant::now(),
        };

        self.peers.insert(peer_id, entry);
    }

    pub(crate) fn get(&self, peer_id: &PeerId) -> Option<PeerInfo> {
        self.peers.get(peer_id).map(|entry| entry.clone())
    }
}

impl Default for PeerStore {
    fn default() -> Self {
        Self::new()
    }
}
