use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_network::NetworkManager;

use crate::node_state::PeerRegistry;

#[derive(Clone)]
pub(crate) struct ShardingTableCheckDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
}
