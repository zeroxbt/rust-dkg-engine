mod config;
mod selection;
mod state;
mod updates;

use dashmap::DashMap;
use dkg_network::PeerId;

use state::PeerRecord;

pub struct PeerRegistry {
    peers: DashMap<PeerId, PeerRecord>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self {
            peers: DashMap::new(),
        }
    }
}

impl Default for PeerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
