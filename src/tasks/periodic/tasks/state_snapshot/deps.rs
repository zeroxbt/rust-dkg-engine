use std::sync::Arc;

use dkg_repository::{KcChainMetadataRepository, KcProjectionRepository, KcSyncRepository};

use crate::peer_registry::PeerRegistry;

#[derive(Clone)]
pub(crate) struct StateSnapshotDeps {
    pub(crate) kc_sync_repository: KcSyncRepository,
    pub(crate) kc_chain_metadata_repository: KcChainMetadataRepository,
    pub(crate) kc_projection_repository: KcProjectionRepository,
    pub(crate) peer_registry: Arc<PeerRegistry>,
}
