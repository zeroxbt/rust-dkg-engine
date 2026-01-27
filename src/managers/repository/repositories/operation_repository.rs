use std::sync::Arc;

use chrono::Utc;
use sea_orm::{ActiveModelTrait, DatabaseConnection, EntityTrait, Set};
use uuid::Uuid;

use crate::managers::repository::{
    error::RepositoryError,
    models::operations::{self, Entity, Model},
    types::OperationStatus,
};

pub(crate) struct OperationRepository {
    conn: Arc<DatabaseConnection>,
}

impl OperationRepository {
    pub(crate) fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Create a new operation record
    pub(crate) async fn create(
        &self,
        operation_id: Uuid,
        operation_name: &str,
        status: OperationStatus,
        timestamp: i64,
    ) -> Result<Model, RepositoryError> {
        let now = Utc::now();

        let active_model = operations::ActiveModel {
            operation_id: Set(operation_id.to_string()),
            operation_name: Set(operation_name.to_string()),
            status: Set(status.as_str().to_string()),
            error_message: Set(None),
            timestamp: Set(timestamp),
            total_peers: Set(None),
            min_ack_responses: Set(None),
            completed_count: Set(0),
            failed_count: Set(0),
            created_at: Set(now),
            updated_at: Set(now),
        };

        let result = Entity::insert(active_model)
            .exec_with_returning(self.conn.as_ref())
            .await?;

        Ok(result)
    }

    /// Get an operation record by its ID
    pub(crate) async fn get(&self, operation_id: Uuid) -> Result<Option<Model>, RepositoryError> {
        let record = Entity::find_by_id(operation_id.to_string())
            .one(self.conn.as_ref())
            .await?;

        Ok(record)
    }

    /// Update an operation record with flexible field updates
    pub(crate) async fn update(
        &self,
        operation_id: Uuid,
        status: Option<OperationStatus>,
        error_message: Option<String>,
        timestamp: Option<i64>,
    ) -> Result<Model, RepositoryError> {
        // First, find the existing record
        let existing = Entity::find_by_id(operation_id.to_string())
            .one(self.conn.as_ref())
            .await?
            .ok_or_else(|| {
                RepositoryError::NotFound(format!("Operation {} not found", operation_id))
            })?;

        let mut active_model: operations::ActiveModel = existing.into();

        // Update fields if provided
        if let Some(s) = status {
            active_model.status = Set(s.as_str().to_string());
        }
        if let Some(em) = error_message {
            active_model.error_message = Set(Some(em));
        }
        if let Some(ts) = timestamp {
            active_model.timestamp = Set(ts);
        }

        active_model.updated_at = Set(Utc::now());

        let result = active_model.update(self.conn.as_ref()).await?;

        Ok(result)
    }

    /// Update only the status field
    pub(crate) async fn update_status(
        &self,
        operation_id: Uuid,
        status: OperationStatus,
    ) -> Result<Model, RepositoryError> {
        self.update(operation_id, Some(status), None, None).await
    }

    /// Update progress counters (completed_count and failed_count)
    pub(crate) async fn update_progress(
        &self,
        operation_id: Uuid,
        completed_count: u16,
        failed_count: u16,
    ) -> Result<Model, RepositoryError> {
        let existing = Entity::find_by_id(operation_id.to_string())
            .one(self.conn.as_ref())
            .await?
            .ok_or_else(|| {
                RepositoryError::NotFound(format!("Operation {} not found", operation_id))
            })?;

        let mut active_model: operations::ActiveModel = existing.into();
        active_model.completed_count = Set(completed_count);
        active_model.failed_count = Set(failed_count);
        active_model.updated_at = Set(Utc::now());

        let result = active_model.update(self.conn.as_ref()).await?;
        Ok(result)
    }
}
