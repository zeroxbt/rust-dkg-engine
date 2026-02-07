use std::{sync::Arc, time::Duration};

use super::{
    finality_acks::cleanup_finality_acks, kc_sync_queue::cleanup_kc_sync_queue,
    operations::cleanup_operations, pending_storage::cleanup_pending_storage,
    proof_challenges::cleanup_proof_challenges,
};
use crate::{
    commands::{
        executor::CommandExecutionResult, periodic::cleanup::CleanupConfig,
        registry::CommandHandler,
    },
    context::Context,
    managers::repository::RepositoryManager,
    services::{OperationStatusService, PendingStorageService},
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
    pending_storage_service: Arc<PendingStorageService>,
    publish_operation_results:
        Arc<OperationStatusService<crate::operations::PublishStoreOperationResult>>,
    get_operation_results: Arc<OperationStatusService<crate::operations::GetOperationResult>>,
}

impl CleanupCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
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

        if data.config.pending_storage.ttl_secs > 0 {
            match cleanup_pending_storage(
                &self.pending_storage_service,
                Duration::from_secs(data.config.pending_storage.ttl_secs),
                data.config.pending_storage.batch_size,
            ) {
                Ok(removed) => {
                    if removed > 0 {
                        tracing::info!(removed, "Cleaned up pending storage entries");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "Failed to clean pending storage entries"),
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

        if data.config.kc_sync_queue.ttl_secs > 0 {
            match cleanup_kc_sync_queue(
                &self.repository_manager,
                Duration::from_secs(data.config.kc_sync_queue.ttl_secs),
                data.config.kc_sync_queue.max_retries,
                data.config.kc_sync_queue.batch_size,
            )
            .await
            {
                Ok(removed) => {
                    if removed > 0 {
                        tracing::info!(removed, "Cleaned up KC sync queue entries");
                    }
                }
                Err(e) => tracing::warn!(error = %e, "Failed to clean KC sync queue entries"),
            }
        }

        CommandExecutionResult::Repeat { delay: interval }
    }
}
