use async_trait::async_trait;
use std::{
    collections::HashMap,
    fmt::{self, Display},
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use blockchain::BlockchainName;
use network::{
    action::NetworkAction,
    message::{RequestMessage, ResponseMessageType},
    PeerId,
};
use repository::RepositoryManager;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::{mpsc::Sender, Mutex};
use uuid::Uuid;

use crate::{
    error::{NodeError, ServiceError},
    services::file_service::{FileService, FileServiceError},
};

use super::sharding_table_service::ShardingTableService;

type Result<T> = std::result::Result<T, NodeError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationId(Uuid);

impl OperationId {
    /// Generate a new operation ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Get the inner UUID (useful for database operations)
    pub fn into_inner(self) -> Uuid {
        self.0
    }
}

impl Default for OperationId {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for OperationId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

impl From<Uuid> for OperationId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<OperationId> for Uuid {
    fn from(id: OperationId) -> Self {
        id.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProtocolOperation {
    Publish,
    Get,
    Update,
}

#[derive(Debug, Clone)]
pub struct OperationState<OperationRequestMessageData: Clone> {
    pub nodes_found: Vec<PeerId>,
    pub last_contacted_index: usize,
    pub failed_number: usize,
    pub completed_number: usize,
    pub request_message: RequestMessage<OperationRequestMessageData>,
}

#[derive(Default)]
pub struct OperationResponseTracker<OperationRequestMessageData: Clone> {
    operation_states: Arc<Mutex<HashMap<Uuid, OperationState<OperationRequestMessageData>>>>,
}

impl<OperationRequestMessageData: Clone> OperationResponseTracker<OperationRequestMessageData> {
    pub fn new() -> Self {
        Self {
            operation_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn insert_operation_state(
        &self,
        operation_id: Uuid,
        operation_state: OperationState<OperationRequestMessageData>,
    ) {
        let mut operation_states = self.operation_states.lock().await;
        operation_states.insert(operation_id, operation_state);
    }

    async fn get_or_create_state(
        &self,
        operation_id: &Uuid,
    ) -> OperationState<OperationRequestMessageData> {
        let mut operation_states = self.operation_states.lock().await;

        operation_states.get_mut(operation_id).unwrap().clone()
    }

    async fn update_state_responses(&self, operation_id: &Uuid, is_successful: bool) {
        let mut operation_states = self.operation_states.lock().await;
        let state = operation_states.get_mut(operation_id).unwrap();

        if is_successful {
            state.completed_number += 1;
        } else {
            state.failed_number += 1;
        }
    }

    async fn update_state_last_contacted_index(&self, operation_id: &Uuid, new_index: usize) {
        let mut operation_states = self.operation_states.lock().await;
        let state = operation_states.get_mut(operation_id).unwrap();

        state.last_contacted_index += new_index;
    }
}

/// Trait for network operation protocol - handles batch sending and response tracking
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
    type RequestData: Serialize + DeserializeOwned + Send + Sync;
    type ResponseData: Serialize + DeserializeOwned + Send + Sync;

    fn repository_manager(&self) -> &Arc<RepositoryManager>;
    fn file_service(&self) -> &Arc<FileService>;

    async fn create_operation_record(&self, operation_name: &str) -> Result<OperationId> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let operation_id = OperationId::new();
        // TODO: add proper statuses
        self.repository_manager()
            .operation_repository()
            .create(
                operation_id.into_inner(),
                operation_name,
                OperationStatus::InProgress.as_str(),
                now_ms,
            )
            .await?;

        tracing::debug!("Generated operation id for request {operation_id}");

        Ok(operation_id)
    }

    /// Cache request data
    async fn cache_request(
        &self,
        operation_id: OperationId,
        data: &Self::RequestData,
    ) -> Result<()> {
        tracing::debug!("Caching data for operation id {} in file", operation_id);

        let cache_dir = self.file_service().operation_request_cache_dir();

        self.file_service()
            .write_json(&cache_dir, &operation_id.to_string(), &data)
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        Ok(())
    }

    /// Cache response data
    async fn cache_response(
        &self,
        operation_id: OperationId,
        data: &Self::ResponseData,
    ) -> Result<()> {
        tracing::debug!("Caching data for operation id {} in file", operation_id);

        let cache_dir = self.file_service().operation_response_cache_dir();

        self.file_service()
            .write_json(&cache_dir, &operation_id.to_string(), &data)
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;
        Ok(())
    }

    /// Get cached request data
    async fn get_cached_request(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<Self::RequestData>> {
        let file_path = self
            .file_service()
            .operation_request_cache_path(&operation_id.to_string());

        match self.file_service().read_json(&file_path).await {
            Ok(request_data) => Ok(Some(request_data)),
            Err(FileServiceError::FileNotFound(_)) => Ok(None),
            Err(e) => Err(NodeError::Service(ServiceError::Other(e.to_string()))),
        }
    }

    /// Get cached response data
    async fn get_cached_response(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<Self::ResponseData>> {
        let file_path = self
            .file_service()
            .operation_response_cache_path(&operation_id.to_string());

        match self.file_service().read_json(&file_path).await {
            Ok(response_data) => Ok(Some(response_data)),
            Err(FileServiceError::FileNotFound(_)) => Ok(None),
            Err(e) => Err(NodeError::Service(ServiceError::Other(e.to_string()))),
        }
    }

    /// Mark operation as completed and cache response data
    async fn mark_completed(&self, operation_id: OperationId) -> Result<()> {
        // TODO: add proper statuses
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

        Ok(())
    }

    /// Mark operation as failed and remove cache
    async fn mark_failed(&self, operation_id: OperationId, error_msg: String) -> Result<()> {
        self.repository_manager()
            .operation_repository()
            .update(
                operation_id.into_inner(),
                Some(OperationStatus::Failed.as_str()),
                Some(error_msg),
                None,
                None,
            )
            .await?;

        // Remove cache files on failure
        self.file_service()
            .remove_file(
                &self
                    .file_service()
                    .operation_request_cache_path(&operation_id.to_string()),
            )
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;
        self.file_service()
            .remove_file(
                &self
                    .file_service()
                    .operation_response_cache_path(&operation_id.to_string()),
            )
            .await
            .map_err(|e| NodeError::Service(ServiceError::Other(e.to_string())))?;

        Ok(())
    }

    /*  /// Remove expired operation data from file cache
    /// Returns the number of files deleted
    async fn remove_expired_file_cache(
        &self,
        cache_dir_path: &PathBuf,
        expired_timeout_ms: u128,
        batch_size: usize,
    ) -> Result<usize> {
        // Check if cache directory exists
        let entries = match self.file_service().read_dir(&cache_dir_path).await {
            Ok(e) => e,
            Err(_) => return Ok(0), // Directory doesn't exist, nothing to clean
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let mut total_deleted = 0;

        // Process files in batches
        for chunk in entries.chunks(batch_size) {
            let mut tasks = Vec::new();

            for filename in chunk {
                let file_path = cache_dir_path.join(filename);
                let file_service = self.file_service().clone();

                let metadata = self.file_service().metadata(&file_path).await;
                let task = tokio::spawn(async move {
                    match metadata {
                        Ok(metadata) => {
                            if let Ok(modified) = metadata.modified() {
                                let modified_timestamp = modified
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis();

                                if modified_timestamp + expired_timeout_ms < now {
                                    return file_service
                                        .remove_file(&file_path)
                                        .await
                                        .unwrap_or(false);
                                }
                            }
                        }
                        Err(_) => return false,
                    }
                    false
                });

                tasks.push(task);
            }

            // Wait for batch to complete
            let results = futures::future::join_all(tasks).await;
            total_deleted += results
                .iter()
                .filter(|r| *r.as_ref().unwrap_or(&false))
                .count();
        }

        if total_deleted > 0 {
            tracing::debug!("Removed {} expired files from cache", total_deleted);
        }

        Ok(total_deleted)
    } */
}
