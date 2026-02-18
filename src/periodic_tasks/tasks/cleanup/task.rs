use std::{sync::Arc, time::Duration};

use dkg_key_value_store::PublishTmpDatasetStore;
use dkg_network::{BatchGetAck, FinalityAck, GetAck, StoreAck};
use dkg_repository::{FinalityStatusRepository, OperationRepository, ProofChallengeRepository};
use tokio_util::sync::CancellationToken;

use super::{
    CleanupConfig, finality_acks::cleanup_finality_acks, operations::cleanup_operations,
    proof_challenges::cleanup_proof_challenges, publish_tmp_dataset::cleanup_publish_tmp_datasets,
};
use crate::{
    operations::{GetOperation, PublishStoreOperation},
    periodic_tasks::CleanupDeps,
    periodic_tasks::runner::run_with_shutdown,
    services::OperationStatusService,
    state::ResponseChannels,
};

pub(crate) struct CleanupTask {
    operation_repository: OperationRepository,
    finality_status_repository: FinalityStatusRepository,
    proof_challenge_repository: ProofChallengeRepository,
    publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    publish_operation_results: Arc<OperationStatusService<PublishStoreOperation>>,
    get_operation_results: Arc<OperationStatusService<GetOperation>>,
    store_response_channels: Arc<ResponseChannels<StoreAck>>,
    get_response_channels: Arc<ResponseChannels<GetAck>>,
    finality_response_channels: Arc<ResponseChannels<FinalityAck>>,
    batch_get_response_channels: Arc<ResponseChannels<BatchGetAck>>,
    config: CleanupConfig,
}

impl CleanupTask {
    pub(crate) fn new(deps: CleanupDeps, config: CleanupConfig) -> Self {
        Self {
            operation_repository: deps.operation_repository,
            finality_status_repository: deps.finality_status_repository,
            proof_challenge_repository: deps.proof_challenge_repository,
            publish_tmp_dataset_store: deps.publish_tmp_dataset_store,
            publish_operation_results: deps.publish_operation_results,
            get_operation_results: deps.get_operation_results,
            store_response_channels: deps.store_response_channels,
            get_response_channels: deps.get_response_channels,
            finality_response_channels: deps.finality_response_channels,
            batch_get_response_channels: deps.batch_get_response_channels,
            config,
        }
    }

    pub(crate) async fn run(self, shutdown: CancellationToken) {
        run_with_shutdown("cleanup", shutdown, || self.execute(&self.config)).await;
    }

    #[tracing::instrument(name = "periodic_tasks.cleanup", skip(self, config))]
    async fn execute(&self, config: &CleanupConfig) -> Duration {
        let interval = Duration::from_secs(config.interval_secs);

        if !config.enabled {
            tracing::debug!("Cleanup disabled by configuration");
            return interval;
        }

        if config.operations.ttl_secs > 0 {
            match cleanup_operations(
                &self.operation_repository,
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
                &self.finality_status_repository,
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
                &self.proof_challenge_repository,
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

        let removed_response_channels = self.cleanup_response_channels();
        if removed_response_channels > 0 {
            tracing::info!(
                removed = removed_response_channels,
                "Cleaned up expired network response channels"
            );
        }

        interval
    }

    fn cleanup_response_channels(&self) -> usize {
        self.store_response_channels.cleanup_expired()
            + self.get_response_channels.cleanup_expired()
            + self.finality_response_channels.cleanup_expired()
            + self.batch_get_response_channels.cleanup_expired()
    }
}
