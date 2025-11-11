use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};

use blockchain::BlockchainName;
use network::{
    action::NetworkAction,
    message::{RequestMessage, ResponseMessageType},
    PeerId,
};
use repository::RepositoryManager;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::Sender, Mutex};
use uuid::Uuid;

use super::sharding_table_service::ShardingTableService;

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
    pub init_message: RequestMessage<OperationRequestMessageData>,
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

#[async_trait]
pub trait OperationService {
    type OperationRequestMessageData: Send + Sync + Clone;
    const BATCH_SIZE: usize;
    const MIN_ACK_RESPONSES: usize;

    fn new(
        network_action_tx: Sender<NetworkAction>,
        repository_manager: Arc<RepositoryManager>,
        sharding_table_service: Arc<ShardingTableService>,
    ) -> Self;
    fn repository_manager(&self) -> &Arc<RepositoryManager>;
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
        init_message: RequestMessage<Self::OperationRequestMessageData>,
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
            init_message,
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
                .send(self.create_network_action(peer, operation_state.init_message.clone()))
                .await
                .unwrap();
        }
    }

    async fn handle_init_response(&self, operation_id: Uuid, peer: PeerId) {
        let operation_state = self
            .response_tracker()
            .get_or_create_state(&operation_id)
            .await;

        self.network_action_tx()
            .send(self.create_network_action(peer, operation_state.request_message))
            .await
            .unwrap();
    }
}
