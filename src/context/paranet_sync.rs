use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_repository::RepositoryManager;

use crate::services::{GetFetchService, TripleStoreService};

#[derive(Clone)]
pub(crate) struct ParanetSyncDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) repository_manager: Arc<RepositoryManager>,
    pub(crate) triple_store_service: Arc<TripleStoreService>,
    pub(crate) get_fetch_service: Arc<GetFetchService>,
}
