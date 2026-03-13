use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_network::NetworkManager;
use dkg_repository::ProofChallengeRepository;

use crate::{
    application::{AssertionValidation, TripleStoreAssertions},
    peer_registry::PeerRegistry,
};

#[derive(Clone)]
pub(crate) struct ProvingDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) proof_challenge_repository: ProofChallengeRepository,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) assertion_validation: Arc<AssertionValidation>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
}
