use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_key_value_store::PublishTmpDatasetStore;
use dkg_network::NetworkManager;
use dkg_repository::{KcChainMetadataRepository, KcProjectionRepository, KcSyncRepository};

use crate::{
    application::{AssertionValidation, KcMaterializationService, TripleStoreAssertions},
    commands::scheduler::CommandScheduler,
    node_state::PeerRegistry,
};

#[derive(Clone)]
pub(crate) struct DkgSyncDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) kc_sync_repository: KcSyncRepository,
    pub(crate) kc_projection_repository: KcProjectionRepository,
    pub(crate) kc_chain_metadata_repository: KcChainMetadataRepository,
    pub(crate) publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    pub(crate) command_scheduler: CommandScheduler,
    pub(crate) kc_materialization_service: Arc<KcMaterializationService>,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
    pub(crate) network_manager: Arc<NetworkManager>,
    pub(crate) assertion_validation: Arc<AssertionValidation>,
    pub(crate) peer_registry: Arc<PeerRegistry>,
}
