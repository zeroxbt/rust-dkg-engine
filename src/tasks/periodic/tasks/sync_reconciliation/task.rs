use std::{sync::Arc, time::Duration};

use dkg_blockchain::BlockchainId;
use dkg_repository::{KcChainMetadataRepository, KcProjectionRepository, KcSyncRepository};
use tokio_util::sync::CancellationToken;

use super::SyncReconciliationConfig;
use crate::{
    application::TripleStoreAssertions, tasks::periodic::SyncReconciliationDeps,
    tasks::periodic::runner::run_with_shutdown,
};

pub(crate) struct SyncReconciliationTask {
    config: SyncReconciliationConfig,
    kc_sync_repository: KcSyncRepository,
    kc_projection_repository: KcProjectionRepository,
    kc_chain_metadata_repository: KcChainMetadataRepository,
    triple_store_assertions: Arc<TripleStoreAssertions>,
}

impl SyncReconciliationTask {
    pub(crate) fn new(deps: SyncReconciliationDeps, config: SyncReconciliationConfig) -> Self {
        Self {
            config,
            kc_sync_repository: deps.kc_sync_repository,
            kc_projection_repository: deps.kc_projection_repository,
            kc_chain_metadata_repository: deps.kc_chain_metadata_repository,
            triple_store_assertions: deps.triple_store_assertions,
        }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("sync_reconciliation", shutdown, || {
            self.execute(blockchain_id)
        })
        .await;
    }

    #[tracing::instrument(
        name = "periodic_tasks.sync_reconciliation",
        skip(self),
        fields(blockchain_id = %blockchain_id)
    )]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let interval = Duration::from_secs(self.config.interval_secs.max(1));
        if !self.config.enabled {
            return interval;
        }

        let _ = (
            &self.kc_sync_repository,
            &self.kc_projection_repository,
            &self.kc_chain_metadata_repository,
            &self.triple_store_assertions,
        );

        tracing::debug!(
            blockchain_id = %blockchain_id,
            batch_size = self.config.batch_size,
            "Sync reconciliation skeleton tick"
        );

        interval
    }
}
