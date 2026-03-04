use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_repository::BlockchainRepository;

#[derive(Clone)]
pub(crate) struct BlockchainAdminEventsDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) blockchain_repository: BlockchainRepository,
}
