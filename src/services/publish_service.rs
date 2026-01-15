use std::sync::Arc;

use repository::RepositoryManager;

use super::operation_manager::{OperationConfig, OperationManager};
use crate::{error::NodeError, services::file_service::FileService, types::models::OperationId};

pub struct PublishService {
    operation_manager: Arc<OperationManager>,
    file_service: Arc<FileService>,
}

impl PublishService {
    pub fn new(
        repository_manager: Arc<RepositoryManager>,
        file_service: Arc<FileService>,
    ) -> Self {
        let config = OperationConfig {
            operation_name: Self::OPERATION_NAME,
        };

        Self {
            operation_manager: Arc::new(OperationManager::new(
                repository_manager,
                Arc::clone(&file_service),
                config,
            )),
            file_service,
        }
    }
}

impl PublishService {
    const MIN_ACK_RESPONSES: u8 = 8;
    const OPERATION_NAME: &'static str = "publish";

    pub fn min_ack_responses(&self) -> u8 {
        Self::MIN_ACK_RESPONSES
    }

    pub fn operation_manager(&self) -> &Arc<OperationManager> {
        &self.operation_manager
    }

    pub fn file_service(&self) -> &Arc<FileService> {
        &self.file_service
    }

    /// Mark operation as failed
    pub async fn mark_failed(
        &self,
        operation_id: OperationId,
        error_message: &str,
    ) -> Result<(), NodeError> {
        self.operation_manager
            .mark_failed(operation_id.into_inner(), error_message.to_string())
            .await
    }
}
