use std::sync::Arc;

use dkg_blockchain::BlockchainManager;
use dkg_repository::ParanetKcSyncRepository;

use crate::application::{GetAssertionUseCase, KcMaterializationService};

#[derive(Clone)]
pub(crate) struct ParanetSyncDeps {
    pub(crate) blockchain_manager: Arc<BlockchainManager>,
    pub(crate) paranet_kc_sync_repository: ParanetKcSyncRepository,
    pub(crate) kc_materialization_service: Arc<KcMaterializationService>,
    pub(crate) get_assertion_use_case: Arc<GetAssertionUseCase>,
}
