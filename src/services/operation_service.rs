use async_trait::async_trait;
use std::{
    collections::HashMap,
    fmt::{self, Display},
    str::FromStr,
    sync::Arc,
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

use super::{
    operation_cache_service::OperationCacheService, sharding_table_service::ShardingTableService,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

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

/// Trait for operation lifecycle management - handles caching and operation state
#[async_trait]
pub trait OperationLifecycle {
    type RequestData: Serialize + DeserializeOwned + Send + Sync;
    type ResponseData: Serialize + DeserializeOwned + Send + Sync;

    fn repository_manager(&self) -> &Arc<RepositoryManager>;
    fn operation_cache(&self) -> &Arc<OperationCacheService>;

    /// Cache request data (memory + file)
    async fn cache_request(&self, op_id: Uuid, data: &Self::RequestData) -> Result<()> {
        let json = serde_json::to_string(data)?;
        self.operation_cache()
            .cache_to_memory(op_id.into(), json.clone())
            .await?;
        self.operation_cache()
            .cache_to_file(op_id.into(), json)
            .await?;
        Ok(())
    }

    /// Cache response data (memory + file)
    async fn cache_response(&self, op_id: Uuid, data: &Self::ResponseData) -> Result<()> {
        let json = serde_json::to_string(data)?;
        self.operation_cache()
            .cache_to_memory(op_id.into(), json.clone())
            .await?;
        self.operation_cache()
            .cache_to_file(op_id.into(), json)
            .await?;
        Ok(())
    }

    /// Get cached data (tries memory first, then file)
    async fn get_cached(&self, op_id: Uuid) -> Result<Option<Self::ResponseData>> {
        match self.operation_cache().get_cached(op_id.into()).await? {
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
            None => Ok(None),
        }
    }

    /// Mark operation as completed and cache response data
    async fn mark_completed(&self, op_id: Uuid, response: Self::ResponseData) -> Result<()> {
        let json = serde_json::to_string(&response)?;

        self.repository_manager()
            .operation_repository()
            .update(
                op_id,
                None,
                Some("completed"),
                None,
                Some(json),
                None,
                None,
                None,
            )
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        self.cache_response(op_id, &response).await?;
        Ok(())
    }

    /// Mark operation as failed and remove cache
    async fn mark_failed(&self, op_id: Uuid, error_msg: String) -> Result<()> {
        self.repository_manager()
            .operation_repository()
            .update(
                op_id,
                None,
                Some("failed"),
                None,
                None,
                Some(error_msg),
                None,
                None,
            )
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        self.operation_cache().remove_cache(op_id.into()).await?;
        Ok(())
    }
}
