use std::sync::Arc;

use chrono::Utc;
use network::{NetworkManager, PeerId, message::RequestMessage};
use repository::RepositoryManager;

use super::{
    operation_response_tracker::OperationResponseTracker,
    sharding_table_service::ShardingTableService,
};
use crate::{
    error::{NodeError, ServiceError},
    network::{NetworkProtocols, ProtocolRequest},
    services::file_service::{FileService, FileServiceError},
    types::{
        models::{OperationId, operation::OperationStatus},
        protocol::StoreRequestData,
    },
};

pub struct PublishService {
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    repository_manager: Arc<RepositoryManager>,
    response_tracker: Arc<OperationResponseTracker>,
    sharding_table_service: Arc<ShardingTableService>,
    file_service: Arc<FileService>,
}

impl PublishService {
    pub fn new(
        network_manager: Arc<NetworkManager<NetworkProtocols>>,
        repository_manager: Arc<RepositoryManager>,
        sharding_table_service: Arc<ShardingTableService>,
        file_service: Arc<FileService>,
    ) -> Self {
        Self {
            network_manager,
            repository_manager,
            response_tracker: Arc::new(OperationResponseTracker::new()),
            sharding_table_service,
            file_service,
        }
    }
}

impl PublishService {
    const BATCH_SIZE: u8 = 20;
    const MIN_ACK_RESPONSES: u8 = 8;
    const OPERATION_NAME: &'static str = "publish";

    pub fn min_ack_responses(&self) -> u8 {
        Self::MIN_ACK_RESPONSES
    }

    fn sharding_table_service(&self) -> &Arc<ShardingTableService> {
        &self.sharding_table_service
    }

    fn network_manager(&self) -> &Arc<NetworkManager<NetworkProtocols>> {
        &self.network_manager
    }

    pub fn response_tracker(&self) -> &Arc<OperationResponseTracker> {
        &self.response_tracker
    }

    async fn send_request(&self, peer: PeerId, message: RequestMessage<StoreRequestData>) {
        self.network_manager
            .send_protocol_request(ProtocolRequest::store(peer, message))
            .await
            .unwrap();
    }

    pub async fn create_operation_record(&self) -> Result<OperationId, NodeError> {
        let operation_id = OperationId::new();
        // TODO: add proper statuses
        self.repository_manager
            .operation_repository()
            .create(
                operation_id.into_inner(),
                Self::OPERATION_NAME,
                OperationStatus::InProgress.as_str(),
                Utc::now().timestamp_millis(),
            )
            .await?;

        tracing::debug!("Generated operation id for request {operation_id}");

        Ok(operation_id)
    }

    /// Get cached result data
    async fn get_cached_result(&self, operation_id: OperationId) -> Result<Option<()>, NodeError> {
        let file_path = self
            .file_service
            .operation_result_cache_path(&operation_id.to_string());

        match self.file_service.read_json(&file_path).await {
            Ok(result_data) => Ok(Some(result_data)),
            Err(FileServiceError::FileNotFound(_)) => Ok(None),
            Err(e) => Err(NodeError::Service(ServiceError::Other(e.to_string()))),
        }
    }

    /// Mark operation as completed
    pub async fn mark_completed(&self, operation_id: OperationId) -> Result<(), NodeError> {
        tracing::info!(
            "Finalizing {} for operationId: {operation_id}",
            Self::OPERATION_NAME
        );
        self.repository_manager
            .operation_repository()
            .update(
                operation_id.into_inner(),
                Some(OperationStatus::Completed.as_str()),
                None,
                None,
            )
            .await?;

        Ok(())
    }

    /// Mark operation as failed
    pub async fn mark_failed(
        &self,
        operation_id: OperationId,
        error_message: &str,
    ) -> Result<(), NodeError> {
        tracing::warn!(
            "{} for operationId: ${operation_id} failed.",
            Self::OPERATION_NAME
        );
        self.repository_manager
            .operation_repository()
            .update(
                operation_id.into_inner(),
                Some(OperationStatus::Failed.as_str()),
                Some(error_message.to_string()),
                None,
            )
            .await?;

        let operation_result_file_path = self
            .file_service
            .operation_result_cache_path(&operation_id.to_string());

        self.file_service
            .remove_file(&operation_result_file_path)
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        Ok(())
    }
}
