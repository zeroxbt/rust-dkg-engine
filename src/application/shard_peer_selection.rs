use std::sync::Arc;

use dkg_blockchain::BlockchainId;
use dkg_network::{NetworkManager, PeerId};

use crate::node_state::PeerRegistry;

pub(crate) struct ShardPeerSelection {
    network_manager: Arc<NetworkManager>,
    peer_registry: Arc<PeerRegistry>,
}

impl ShardPeerSelection {
    pub(crate) fn new(
        network_manager: Arc<NetworkManager>,
        peer_registry: Arc<PeerRegistry>,
    ) -> Self {
        Self {
            network_manager,
            peer_registry,
        }
    }

    pub(crate) fn is_local_node_in_shard(&self, blockchain_id: &BlockchainId) -> bool {
        let peer_id = self.network_manager.peer_id();
        self.peer_registry.is_peer_in_shard(blockchain_id, peer_id)
    }

    pub(crate) fn load_shard_peers(
        &self,
        blockchain_id: &BlockchainId,
        protocol: &'static str,
    ) -> Vec<PeerId> {
        let my_peer_id = self.network_manager.peer_id();
        let mut peers =
            self.peer_registry
                .select_shard_peers(blockchain_id, protocol, Some(my_peer_id));
        self.peer_registry.sort_by_latency(&mut peers);
        peers
    }
}
