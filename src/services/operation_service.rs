use std::{collections::HashMap, sync::Arc};

use network::{PeerId, message::RequestMessage};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::error::NodeError;

type Result<T> = std::result::Result<T, NodeError>;

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

    pub(crate) async fn insert_operation_state(
        &self,
        operation_id: Uuid,
        operation_state: OperationState<OperationRequestMessageData>,
    ) {
        let mut operation_states = self.operation_states.lock().await;
        operation_states.insert(operation_id, operation_state);
    }

    pub(crate) async fn get_or_create_state(
        &self,
        operation_id: &Uuid,
    ) -> OperationState<OperationRequestMessageData> {
        let mut operation_states = self.operation_states.lock().await;

        operation_states.get_mut(operation_id).unwrap().clone()
    }

    pub(crate) async fn update_state_responses(&self, operation_id: &Uuid, is_successful: bool) {
        let mut operation_states = self.operation_states.lock().await;
        let state = operation_states.get_mut(operation_id).unwrap();

        if is_successful {
            state.completed_number += 1;
        } else {
            state.failed_number += 1;
        }
    }

    pub(crate) async fn update_state_last_contacted_index(
        &self,
        operation_id: &Uuid,
        new_index: usize,
    ) {
        let mut operation_states = self.operation_states.lock().await;
        let state = operation_states.get_mut(operation_id).unwrap();

        state.last_contacted_index += new_index;
    }
}
