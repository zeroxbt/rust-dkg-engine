use std::sync::Arc;

use dkg_blockchain::BlockchainManager;

use crate::peer_registry::PeerRegistry;

#[derive(Clone)]
pub(crate) struct ShardingTableCheckDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
}
