use std::{sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;

use super::{
    CleanupConfig, finality_acks::cleanup_finality_acks, operations::cleanup_operations,
    proof_challenges::cleanup_proof_challenges, publish_tmp_dataset::cleanup_publish_tmp_datasets,
};
use crate::{
    context::Context,
    managers::{key_value_store::PublishTmpDatasetStore, repository::RepositoryManager},
    operations::{GetOperation, PublishStoreOperation},
    periodic::runner::run_with_shutdown,
    services::OperationStatusService,
};

pub(crate) struct CleanupTask {
    repository_manager: Arc<RepositoryManager>,
    publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    publish_operation_results: Arc<OperationStatusService<PublishStoreOperation>>,
    get_operation_results: Arc<OperationStatusService<GetOperation>>,
    config: CleanupConfig,
}

impl CleanupTask {
    pub(crate) fn new(context: Arc<Context>, config: CleanupConfig) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            publish_tmp_dataset_store: Arc::clone(context.publish_tmp_dataset_store()),
            publish_operation_results: Arc::clone(context.publish_store_operation_status_service()),
            get_operation_results: Arc::clone(context.get_operation_status_service()),
            config,
        }
    }

    pub(crate) async fn run(self, shutdown: CancellationToken) {
        run_with_shutdown("cleanup", shutdown, || self.execute(&self.config)).await;
    }

    #[tracing::instrument(name = "periodic.cleanup", skip(self, config))]
    async fn execute(&self, config: &CleanupConfig) -> Duration {
        let interval = Duration::from_secs(config.interval_secs);

        if !config.enabled {
            tracing::debug!("Cleanup disabled by configuration");
            return interval;
        }

        if config.operations.ttl_secs > 0 {
            match cleanup_operations(
                &self.repository_manager,
                &self.publish_operation_results,
                &self.get_operation_results,
                Duration::from_secs(config.operations.ttl_secs),
                config.operations.batch_size,
            )
            .await
            {
                Ok(removed) => {
                    if removed > 0 {
                        tracing::info!(removed, "Cleaned up operation records");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "Failed to clean operation records"),
            }
        }

        if config.publish_tmp_dataset.ttl_secs > 0 {
            match cleanup_publish_tmp_datasets(
                &self.publish_tmp_dataset_store,
                Duration::from_secs(config.publish_tmp_dataset.ttl_secs),
                config.publish_tmp_dataset.batch_size,
            )
            .await
            {
                Ok(removed) => {
                    if removed > 0 {
                        tracing::info!(removed, "Cleaned up publish tmp dataset entries");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "Failed to clean publish tmp dataset entries"),
            }
        }

        if config.finality_acks.ttl_secs > 0 {
            match cleanup_finality_acks(
                &self.repository_manager,
                Duration::from_secs(config.finality_acks.ttl_secs),
                config.finality_acks.batch_size,
            )
            .await
            {
                Ok(removed) => {
                    if removed > 0 {
                        tracing::info!(removed, "Cleaned up finality ack records");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "Failed to clean finality ack records"),
            }
        }

        if config.proof_challenges.ttl_secs > 0 {
            match cleanup_proof_challenges(
                &self.repository_manager,
                Duration::from_secs(config.proof_challenges.ttl_secs),
                config.proof_challenges.batch_size,
            )
            .await
            {
                Ok(removed) => {
                    if removed > 0 {
                        tracing::info!(removed, "Cleaned up proof challenge records");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "Failed to clean proof challenge records"),
            }
        }

        interval
    }
}
