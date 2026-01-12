use async_trait::async_trait;
use blockchain::BlockchainName;
use chrono::Utc;
use network::{
    action::NetworkAction,
    message::{RequestMessage, ResponseMessageType},
    PeerId,
};
use repository::RepositoryManager;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::{
    error::{NodeError, ServiceError},
    services::{
        file_service::{FileService, FileServiceError},
        operation_service::{OperationResponseTracker, OperationState},
        sharding_table_service::ShardingTableService,
    },
    types::models::OperationId,
};

type Result<T> = std::result::Result<T, NodeError>;

#[async_trait]
pub trait NetworkOperationProtocol {
    type OperationRequestMessageData: Send + Sync + Clone;
    const BATCH_SIZE: usize;
    const MIN_ACK_RESPONSES: usize;

    fn sharding_table_service(&self) -> &Arc<ShardingTableService>;
    fn network_action_tx(&self) -> &Sender<NetworkAction>;
    fn response_tracker(&self) -> &OperationResponseTracker<Self::OperationRequestMessageData>;
    fn create_network_action(
        &self,
        peer: PeerId,
        message: RequestMessage<Self::OperationRequestMessageData>,
    ) -> NetworkAction;

    async fn start_operation(
        &self,
        operation_id: Uuid,
        blockchain: BlockchainName,
        keyword: Vec<u8>,
        hash_function_id: u8,
        request_message: RequestMessage<Self::OperationRequestMessageData>,
    ) {
        let peers = self
            .sharding_table_service()
            .find_neighborhood(blockchain, keyword, 20, hash_function_id, true)
            .await;

        tracing::warn!("Peers found: {:?}", peers);

        // Ensure the operation state is created and initialized
        let operation_state = OperationState {
            nodes_found: peers, // Store the nodes
            last_contacted_index: 0,
            failed_number: 0,
            completed_number: 0,
            request_message,
        };
        self.response_tracker()
            .insert_operation_state(operation_id, operation_state.clone())
            .await;

        self.send_batch_messages(operation_id, operation_state)
            .await;
    }

    async fn handle_request_response(
        &self,
        operation_id: Uuid,
        response_type: ResponseMessageType,
    ) {
        self.response_tracker()
            .update_state_responses(&operation_id, response_type == ResponseMessageType::Ack)
            .await;

        let operation_state = self
            .response_tracker()
            .get_or_create_state(&operation_id)
            .await;

        let total_responses = operation_state.completed_number + operation_state.failed_number;

        tracing::debug!(
            "Processing response for operation id: {}, number of responses: {}, Completed: {}, Failed: {}, minimum replication factor: {}",
            operation_id, total_responses, operation_state.completed_number, operation_state.failed_number, Self::MIN_ACK_RESPONSES
        );

        if operation_state.completed_number == Self::MIN_ACK_RESPONSES
            && response_type == ResponseMessageType::Ack
        {
            tracing::info!("Operation {} is completed", operation_id);
        } else if operation_state.completed_number < Self::MIN_ACK_RESPONSES
            && (operation_state.nodes_found.len() == total_responses
                || ((total_responses) % Self::BATCH_SIZE == 0))
        {
            if operation_state.last_contacted_index >= operation_state.nodes_found.len() - 1 {
                tracing::warn!("Operation {} has failed", operation_id)
            } else {
                tracing::warn!("Not enough nodes responded with ACK. sending next batch...");
                self.send_batch_messages(operation_id, operation_state)
                    .await;
            }
        }
    }

    async fn send_batch_messages(
        &self,
        operation_id: Uuid,
        operation_state: OperationState<Self::OperationRequestMessageData>,
    ) {
        let start_index = operation_state.last_contacted_index;
        let end_index = usize::min(
            start_index + Self::BATCH_SIZE,
            operation_state.nodes_found.len(),
        );

        let peers_to_contact = operation_state.nodes_found[start_index..end_index].to_vec();

        self.response_tracker()
            .update_state_last_contacted_index(&operation_id, end_index)
            .await;

        for peer in peers_to_contact {
            self.network_action_tx()
                .send(self.create_network_action(peer, operation_state.request_message.clone()))
                .await
                .unwrap();
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum OperationStatus {
    InProgress,
    Completed,
    Failed,
}

impl OperationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InProgress => "IN_PROGRESS",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

/// Trait for operation lifecycle management - handles caching and operation state
#[async_trait]
pub trait OperationLifecycle {
    type ResultData: Serialize + DeserializeOwned + Send + Sync;
    const OPERATION_NAME: &'static str;

    fn repository_manager(&self) -> &Arc<RepositoryManager>;
    fn file_service(&self) -> &Arc<FileService>;

    async fn create_operation_record(&self) -> Result<OperationId> {
        let operation_id = OperationId::new();
        // TODO: add proper statuses
        self.repository_manager()
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
    async fn get_cached_result(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<Self::ResultData>> {
        let file_path = self
            .file_service()
            .operation_result_cache_path(&operation_id.to_string());

        match self.file_service().read_json(&file_path).await {
            Ok(result_data) => Ok(Some(result_data)),
            Err(FileServiceError::FileNotFound(_)) => Ok(None),
            Err(e) => Err(NodeError::Service(ServiceError::Other(e.to_string()))),
        }
    }

    /// Mark operation as completed
    async fn mark_completed(
        &self,
        operation_id: OperationId,
        data: &Self::ResultData,
    ) -> Result<()> {
        tracing::info!(
            "Finalizing {} for operationId: {operation_id}",
            Self::OPERATION_NAME
        );
        self.repository_manager()
            .operation_repository()
            .update(
                operation_id.into_inner(),
                Some(OperationStatus::Completed.as_str()),
                None,
                None,
                None,
            )
            .await?;

        tracing::debug!(
            "Caching result data for operation with id {} in file",
            operation_id
        );

        let cache_dir = self.file_service().operation_result_cache_dir();

        self.file_service()
            .write_json(&cache_dir, &operation_id.to_string(), &data)
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        Ok(())
    }

    /// Mark operation as failed
    async fn mark_failed(
        &self,
        operation_id: OperationId,
        error: Box<dyn std::error::Error + Send + Sync>,
    ) -> Result<()> {
        tracing::warn!(
            "{} for operationId: ${operation_id} failed.",
            Self::OPERATION_NAME
        );
        self.repository_manager()
            .operation_repository()
            .update(
                operation_id.into_inner(),
                Some(OperationStatus::Failed.as_str()),
                Some(error.to_string()),
                None,
                None,
            )
            .await?;

        let cache_dir = self.file_service().operation_result_cache_dir();

        self.file_service()
            .write(
                &cache_dir,
                &operation_id.to_string(),
                error.to_string().into_bytes().as_slice(),
            )
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        Ok(())
    }
}
