use std::sync::Arc;

use dkg_repository::{KcChainMetadataRepository, KcProjectionRepository, KcSyncRepository};

use crate::application::TripleStoreAssertions;

#[derive(Clone)]
pub(crate) struct KcReconciliationDeps {
    pub(crate) kc_sync_repository: KcSyncRepository,
    pub(crate) kc_projection_repository: KcProjectionRepository,
    pub(crate) kc_chain_metadata_repository: KcChainMetadataRepository,
    pub(crate) triple_store_assertions: Arc<TripleStoreAssertions>,
}
