use std::sync::Arc;

use dkg_blockchain::BlockchainManager;

#[derive(Clone)]
pub(crate) struct ClaimRewardsDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
}
