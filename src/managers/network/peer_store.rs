use std::time::Instant;

use dashmap::{DashMap, DashSet};
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
    /// Bootstrap peers are always allowed even if they are not in the shard table.
    /// Kept separate so shard allowlist refreshes don't evict bootstrap nodes.
    bootstrap_peers: DashSet<PeerId>,
    allowed_peers: DashSet<PeerId>,
}

impl PeerStore {
    pub(crate) fn new() -> Self {
        Self {
            peers: DashMap::new(),
            bootstrap_peers: DashSet::new(),
            allowed_peers: DashSet::new(),
        }
    }

    pub(crate) fn set_bootstrap_peers<I>(&self, peers: I)
    where
        I: IntoIterator<Item = PeerId>,
    {
        self.bootstrap_peers.clear();
        for peer_id in peers {
            self.bootstrap_peers.insert(peer_id);
        }

        self.prune_disallowed();
    }

    pub(crate) fn set_allowed_peers<I>(&self, peers: I)
    where
        I: IntoIterator<Item = PeerId>,
    {
        self.allowed_peers.clear();
        for peer_id in peers {
            self.allowed_peers.insert(peer_id);
        }

        self.prune_disallowed();
    }

    fn is_allowed(&self, peer_id: &PeerId) -> bool {
        self.bootstrap_peers.contains(peer_id) || self.allowed_peers.contains(peer_id)
    }

    fn prune_disallowed(&self) {
        let mut to_remove = Vec::new();
        for entry in self.peers.iter() {
            if !self.is_allowed(entry.key()) {
                to_remove.push(*entry.key());
            }
        }
        for peer_id in to_remove {
            self.peers.remove(&peer_id);
        }
    }

    pub(crate) fn record_identify(&self, peer_id: PeerId, info: &identify::Info) {
        if !self.is_allowed(&peer_id) {
            return;
        }

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
        if !self.is_allowed(peer_id) {
            return None;
        }
        self.peers.get(peer_id).map(|entry| entry.clone())
    }
}

impl Default for PeerStore {
    fn default() -> Self {
        Self::new()
    }
}
