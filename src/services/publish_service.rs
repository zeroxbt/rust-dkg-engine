use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProtocolOperation {
    Publish,
    Get,
    Update,
}

#[derive(Default, Debug, Clone)]
pub struct OperationStatus {
    pub failed_number: usize,
    pub completed_number: usize,
}

#[derive(Default)]
pub struct OperationResponseTracker {
    operation_statuses: Arc<Mutex<HashMap<Uuid, OperationStatus>>>,
}

impl OperationResponseTracker {
    async fn get_or_create_status(&self, operation_id: Uuid) -> OperationStatus {
        let mut operation_statuses = self.operation_statuses.lock().await;

        // Ensure atomic get-or-create behavior without keeping the lock
        operation_statuses
            .entry(operation_id)
            .or_insert_with(OperationStatus::default)
            .clone()
    }

    async fn update_status(&self, operation_id: Uuid, is_successful: bool) {
        let mut operation_statuses = self.operation_statuses.lock().await;
        let status = operation_statuses
            .entry(operation_id)
            .or_insert_with(OperationStatus::default);

        if is_successful {
            status.completed_number += 1;
        } else {
            status.failed_number += 1;
        }
    }
}

pub struct PublishService {
    response_tracker: OperationResponseTracker,
}

impl PublishService {
    async fn handle_response(&self, operation_id: Uuid) {
        self.response_tracker
            .update_status(operation_id, true)
            .await;

        let operation_status = self
            .response_tracker
            .get_or_create_status(operation_id)
            .await;
        if operation_status.completed_number >= 5 {
            println!("Operation {} is completed", operation_id);
        } else if operation_status.failed_number >= 15 {
            println!("Operation {} has failed", operation_id);
        } else {
            tracing::debug!(
            "Processing publish response for operation id: {}, number of responses: {}, Completed: {}, Failed: {}, minimum replication factor: {}",
            operation_id, operation_status.completed_number + operation_status.failed_number, operation_status.completed_number, operation_status.failed_number, 5
        )
        }
    }
}
