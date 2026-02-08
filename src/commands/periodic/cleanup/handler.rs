use std::{sync::Arc, time::Duration};

use super::{
    finality_acks::cleanup_finality_acks, operations::cleanup_operations,
    proof_challenges::cleanup_proof_challenges, publish_tmp_dataset::cleanup_publish_tmp_datasets,
};
use crate::{
    commands::{
        executor::CommandExecutionResult, periodic::cleanup::CleanupConfig,
        registry::CommandHandler,
    },
    context::Context,
    managers::{key_value_store::PublishTmpDatasetStore, repository::RepositoryManager},
    operations::{GetOperation, PublishStoreOperation},
    services::OperationStatusService,
};

#[derive(Clone)]
pub(crate) struct CleanupCommandData {
    pub config: CleanupConfig,
}

impl CleanupCommandData {
    pub(crate) fn new(config: CleanupConfig) -> Self {
        Self { config }
    }
}

pub(crate) struct CleanupCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    publish_operation_results: Arc<OperationStatusService<PublishStoreOperation>>,
    get_operation_results: Arc<OperationStatusService<GetOperation>>,
}

impl CleanupCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            publish_tmp_dataset_store: Arc::new(
                context
                    .key_value_store_manager()
                    .publish_tmp_dataset_store()
                    .expect("Failed to create publish tmp dataset store"),
            ),
            publish_operation_results: Arc::clone(context.publish_store_operation_status_service()),
            get_operation_results: Arc::clone(context.get_operation_status_service()),
        }
    }
}

impl CommandHandler<CleanupCommandData> for CleanupCommandHandler {
    #[tracing::instrument(name = "periodic.cleanup", skip(self, data))]
    async fn execute(&self, data: &CleanupCommandData) -> CommandExecutionResult {
        let interval = Duration::from_secs(data.config.interval_secs);

        if !data.config.enabled {
            tracing::debug!("Cleanup disabled by configuration");
            return CommandExecutionResult::Repeat { delay: interval };
        }

        if data.config.operations.ttl_secs > 0 {
            match cleanup_operations(
                &self.repository_manager,
                &self.publish_operation_results,
                &self.get_operation_results,
                Duration::from_secs(data.config.operations.ttl_secs),
                data.config.operations.batch_size,
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

        if data.config.publish_tmp_dataset.ttl_secs > 0 {
            match cleanup_publish_tmp_datasets(
                &self.publish_tmp_dataset_store,
                Duration::from_secs(data.config.publish_tmp_dataset.ttl_secs),
                data.config.publish_tmp_dataset.batch_size,
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

        if data.config.finality_acks.ttl_secs > 0 {
            match cleanup_finality_acks(
                &self.repository_manager,
                Duration::from_secs(data.config.finality_acks.ttl_secs),
                data.config.finality_acks.batch_size,
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

        if data.config.proof_challenges.ttl_secs > 0 {
            match cleanup_proof_challenges(
                &self.repository_manager,
                Duration::from_secs(data.config.proof_challenges.ttl_secs),
                data.config.proof_challenges.batch_size,
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

        CommandExecutionResult::Repeat { delay: interval }
    }
}
