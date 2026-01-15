use std::sync::Arc;

use dashmap::DashMap;
use network::PeerId;
use uuid::Uuid;

use crate::error::NodeError;

type Result<T> = std::result::Result<T, NodeError>;

#[derive(Debug, Clone, std::default::Default)]
pub struct OperationState {
    pub nodes_found: Vec<PeerId>,
    pub min_ack_responses: u8,
    pub last_contacted_index: usize,
    pub failed_number: u8,
    pub completed_number: u8,
}

#[derive(Debug, Clone, std::default::Default)]
pub struct OperationResponseTracker {
    operation_states: Arc<DashMap<Uuid, OperationState>>,
}

impl OperationResponseTracker {
    pub fn new() -> Self {
        Self {
            operation_states: Arc::new(DashMap::new()),
        }
    }

    pub async fn insert_operation_state(
        &self,
        operation_id: Uuid,
        operation_state: OperationState,
    ) {
        self.operation_states.insert(operation_id, operation_state);
    }

    pub async fn update_state_responses(&self, operation_id: &Uuid, is_successful: bool) {
        let mut state = self.operation_states.get_mut(operation_id).unwrap();

        if is_successful {
            state.completed_number += 1;
        } else {
            state.failed_number += 1;
        }
    }

    pub async fn get_state(&self, operation_id: &Uuid) -> OperationState {
        self.operation_states.get(operation_id).unwrap().clone()
    }

    /*

    pub(crate) async fn update_state_responses(&self, operation_id: &Uuid, is_successful: bool) {
        let mut operation_states = self.operation_states.lock().await;
        let state = operation_states.get_mut(operation_id).unwrap_or_default();

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
    } */
}
