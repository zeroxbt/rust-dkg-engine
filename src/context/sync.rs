use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_network::NetworkManager;
use dkg_repository::RepositoryManager;

use crate::services::{AssertionValidationService, PeerService, TripleStoreService};

#[derive(Clone)]
pub(crate) struct SyncDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) repository_manager: Arc<RepositoryManager>,
    pub(crate) triple_store_service: Arc<TripleStoreService>,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) assertion_validation_service: Arc<AssertionValidationService>,
    pub(crate) peer_service: Arc<PeerService>,
}
