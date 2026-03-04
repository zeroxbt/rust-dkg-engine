use std::{sync::Arc, time::Duration};

use dkg_blockchain::BlockchainId;
use dkg_repository::{KcChainMetadataRepository, KcProjectionRepository, KcSyncRepository};
use tokio_util::sync::CancellationToken;

use super::{
    KcReconciliationDeps,
    KcReconciliationConfig,
    phases::{cleanup_stale_queue, hydrate_projection, reconcile_non_present, repair_orphans},
};
use crate::{
    application::TripleStoreAssertions,
    tasks::periodic::PeriodicTasksDeps,
    tasks::periodic::registry::ConfiguredBlockchainPeriodicTask,
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

        self.run_hydrate_projection_phase(blockchain_id).await;
        self.run_repair_orphans_phase(blockchain_id).await;
        self.run_cleanup_stale_queue_phase(blockchain_id).await;
        self.run_reconcile_non_present_phase(blockchain_id).await;

        interval
    }

    async fn run_hydrate_projection_phase(&self, blockchain_id: &BlockchainId) {
        let batch_size = self.config.max_kc_rows_per_phase.max(1);
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

    async fn run_reconcile_non_present_phase(&self, blockchain_id: &BlockchainId) {
        let batch_size = self.config.max_kc_rows_per_phase.max(1);
        let phase_outcome = match reconcile_non_present::run(
            blockchain_id,
            batch_size,
            &self.kc_projection_repository,
            &self.kc_chain_metadata_repository,
            &self.kc_sync_repository,
            self.triple_store_assertions.as_ref(),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    batch_size,
                    error = %error,
                    "KC reconciliation non-present phase failed"
                );
                return;
            }
        };

        if phase_outcome.candidates == 0 {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                batch_size,
                "KC reconciliation non-present phase found no candidates"
            );
            return;
        }

        tracing::info!(
            blockchain_id = %blockchain_id,
            batch_size,
            candidates = phase_outcome.candidates,
            found_present = phase_outcome.found_present,
            enqueued = phase_outcome.enqueued,
            metadata_missing = phase_outcome.metadata_missing,
            failed_projection_updates = phase_outcome.failed_projection_updates,
            "KC reconciliation non-present phase completed"
        );
    }

    async fn run_repair_orphans_phase(&self, blockchain_id: &BlockchainId) {
        let batch_size = self.config.max_kc_rows_per_phase.max(1);
        let phase_outcome = match repair_orphans::run(
            blockchain_id,
            batch_size,
            &self.kc_projection_repository,
            &self.kc_sync_repository,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    batch_size,
                    error = %error,
                    "KC reconciliation orphan-repair phase failed"
                );
                return;
            }
        };

        if phase_outcome.queue_missing_projection == 0 && phase_outcome.pending_missing_queue == 0 {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                batch_size,
                "KC reconciliation orphan-repair phase found no orphans"
            );
            return;
        }

        tracing::info!(
            blockchain_id = %blockchain_id,
            batch_size,
            queue_missing_projection = phase_outcome.queue_missing_projection,
            hydrated_pending = phase_outcome.hydrated_pending,
            pending_missing_queue = phase_outcome.pending_missing_queue,
            reset_unknown = phase_outcome.reset_unknown,
            failed_projection_updates = phase_outcome.failed_projection_updates,
            "KC reconciliation orphan-repair phase completed"
        );
    }

    async fn run_cleanup_stale_queue_phase(&self, blockchain_id: &BlockchainId) {
        let batch_size = self.config.max_kc_rows_per_phase.max(1);
        let phase_outcome =
            match cleanup_stale_queue::run(blockchain_id, batch_size, &self.kc_sync_repository)
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        batch_size,
                        error = %error,
                        "KC reconciliation stale-queue cleanup phase failed"
                    );
                    return;
                }
            };

        if phase_outcome.stale_rows == 0 {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                batch_size,
                "KC reconciliation stale-queue cleanup phase found no rows"
            );
            return;
        }

        tracing::info!(
            blockchain_id = %blockchain_id,
            batch_size,
            stale_rows = phase_outcome.stale_rows,
            removed_rows = phase_outcome.removed_rows,
            failed_rows = phase_outcome.failed_rows,
            failed_contracts = phase_outcome.failed_contracts,
            "KC reconciliation stale-queue cleanup phase completed"
        );
    }
}

impl ConfiguredBlockchainPeriodicTask for KcReconciliationTask {
    type Config = KcReconciliationConfig;

    fn from_deps(deps: Arc<PeriodicTasksDeps>, config: Self::Config) -> Self {
        Self::new(deps.kc_reconciliation.clone(), config)
    }

    fn run_task(
        self,
        blockchain_id: &BlockchainId,
        shutdown: CancellationToken,
    ) -> impl std::future::Future<Output = ()> + Send {
        Self::run(self, blockchain_id, shutdown)
    }
}
