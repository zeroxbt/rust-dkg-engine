use std::{sync::Arc, time::Duration};

use dkg_blockchain::BlockchainId;
use dkg_repository::{KcChainMetadataRepository, KcProjectionRepository, KcSyncRepository};
use tokio_util::sync::CancellationToken;

use super::{KcReconciliationConfig, phases::hydrate_projection};
use crate::{
    application::TripleStoreAssertions, tasks::periodic::KcReconciliationDeps,
    tasks::periodic::runner::run_with_shutdown,
};

pub(crate) struct KcReconciliationTask {
    config: KcReconciliationConfig,
    kc_sync_repository: KcSyncRepository,
    kc_projection_repository: KcProjectionRepository,
    kc_chain_metadata_repository: KcChainMetadataRepository,
    triple_store_assertions: Arc<TripleStoreAssertions>,
}

impl KcReconciliationTask {
    pub(crate) fn new(deps: KcReconciliationDeps, config: KcReconciliationConfig) -> Self {
        Self {
            config,
            kc_sync_repository: deps.kc_sync_repository,
            kc_projection_repository: deps.kc_projection_repository,
            kc_chain_metadata_repository: deps.kc_chain_metadata_repository,
            triple_store_assertions: deps.triple_store_assertions,
        }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("kc_reconciliation", shutdown, || {
            self.execute(blockchain_id)
        })
        .await;
    }

    #[tracing::instrument(
        name = "periodic_tasks.kc_reconciliation",
        skip(self),
        fields(blockchain_id = %blockchain_id)
    )]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let interval = Duration::from_secs(self.config.interval_secs.max(1));
        if !self.config.enabled {
            return interval;
        }

        let _ = (&self.kc_sync_repository, &self.triple_store_assertions);
        self.run_hydrate_projection_phase(blockchain_id).await;

        interval
    }

    async fn run_hydrate_projection_phase(&self, blockchain_id: &BlockchainId) {
        let batch_size = self.config.batch_size.max(1);
        let phase_outcome = match hydrate_projection::run(
            blockchain_id,
            batch_size,
            &self.kc_chain_metadata_repository,
            &self.kc_projection_repository,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    batch_size,
                    error = %error,
                    "KC reconciliation hydration phase failed to query missing projection keys"
                );
                return;
            }
        };

        if phase_outcome.missing_keys == 0 {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                batch_size,
                "KC reconciliation hydration phase found no missing projection keys"
            );
            return;
        }

        tracing::info!(
            blockchain_id = %blockchain_id,
            batch_size,
            missing = phase_outcome.missing_keys,
            hydrated = phase_outcome.hydrated_keys,
            failed = phase_outcome.failed_keys,
            failed_contracts = phase_outcome.failed_contracts,
            "KC reconciliation hydration phase completed"
        );
    }
}
