use std::sync::Arc;

use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, sea_query::Expr,
};
use uuid::Uuid;

use crate::{
    error::RepositoryError,
    models::operations::{self, Column, Entity, Model},
};

pub struct OperationRepository {
    conn: Arc<DatabaseConnection>,
}

impl OperationRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Create a new operation record
    pub async fn create(
        &self,
        operation_id: Uuid,
        operation_name: &str,
        status: &str,
        timestamp: i64,
    ) -> Result<Model, RepositoryError> {
        let now = Utc::now();

        let active_model = operations::ActiveModel {
            operation_id: Set(operation_id),
            operation_name: Set(operation_name.to_string()),
            status: Set(status.to_string()),
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
    pub async fn get(&self, operation_id: Uuid) -> Result<Option<Model>, RepositoryError> {
        let record = Entity::find_by_id(operation_id)
            .one(self.conn.as_ref())
            .await?;

        Ok(record)
    }

    /// Update an operation record with flexible field updates
    pub async fn update(
        &self,
        operation_id: Uuid,
        status: Option<&str>,
        error_message: Option<String>,
        timestamp: Option<i64>,
    ) -> Result<Model, RepositoryError> {
        // First, find the existing record
        let existing = Entity::find_by_id(operation_id)
            .one(self.conn.as_ref())
            .await?
            .ok_or_else(|| {
                RepositoryError::NotFound(format!("Operation {} not found", operation_id))
            })?;

        let mut active_model: operations::ActiveModel = existing.into();

        // Update fields if provided
        if let Some(fs) = status {
            active_model.status = Set(fs.to_string());
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
    pub async fn update_status(
        &self,
        operation_id: Uuid,
        status: &str,
    ) -> Result<Model, RepositoryError> {
        self.update(operation_id, Some(status), None, None).await
    }

    /// Update progress counters (completed_count and failed_count)
    pub async fn update_progress(
        &self,
        operation_id: Uuid,
        completed_count: u16,
        failed_count: u16,
    ) -> Result<Model, RepositoryError> {
        let existing = Entity::find_by_id(operation_id)
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

    /// Atomically increment either completed_count or failed_count and return the updated record.
    /// This avoids race conditions when multiple responses arrive concurrently.
    pub async fn atomic_increment_response(
        &self,
        operation_id: Uuid,
        is_success: bool,
    ) -> Result<Model, RepositoryError> {
        // Atomically increment the appropriate counter
        let update_result = if is_success {
            Entity::update_many()
                .filter(Column::OperationId.eq(operation_id))
                .col_expr(
                    Column::CompletedCount,
                    Expr::col(Column::CompletedCount).add(1),
                )
                .col_expr(Column::UpdatedAt, Expr::value(Utc::now()))
                .exec(self.conn.as_ref())
                .await?
        } else {
            Entity::update_many()
                .filter(Column::OperationId.eq(operation_id))
                .col_expr(Column::FailedCount, Expr::col(Column::FailedCount).add(1))
                .col_expr(Column::UpdatedAt, Expr::value(Utc::now()))
                .exec(self.conn.as_ref())
                .await?
        };

        if update_result.rows_affected == 0 {
            return Err(RepositoryError::NotFound(format!(
                "Operation {} not found",
                operation_id
            )));
        }

        // Fetch the updated record to get the new counts
        let record = Entity::find_by_id(operation_id)
            .one(self.conn.as_ref())
            .await?
            .ok_or_else(|| {
                RepositoryError::NotFound(format!("Operation {} not found", operation_id))
            })?;

        Ok(record)
    }

    /// Atomically update status only if current status matches expected status.
    /// Returns Ok(Some(model)) if update succeeded, Ok(None) if status didn't match.
    /// This enables compare-and-swap for status transitions.
    pub async fn atomic_complete_if_in_progress(
        &self,
        operation_id: Uuid,
        new_status: &str,
        error_message: Option<String>,
    ) -> Result<Option<Model>, RepositoryError> {
        let mut update_query = Entity::update_many()
            .filter(Column::OperationId.eq(operation_id))
            .filter(Column::Status.eq("in_progress"))
            .col_expr(Column::Status, Expr::value(new_status))
            .col_expr(Column::UpdatedAt, Expr::value(Utc::now()));

        if let Some(em) = error_message {
            update_query = update_query.col_expr(Column::ErrorMessage, Expr::value(Some(em)));
        }

        let update_result = update_query.exec(self.conn.as_ref()).await?;

        if update_result.rows_affected == 0 {
            // Either not found or status wasn't "in_progress"
            // Check if the record exists
            let record = Entity::find_by_id(operation_id)
                .one(self.conn.as_ref())
                .await?;

            if record.is_none() {
                return Err(RepositoryError::NotFound(format!(
                    "Operation {} not found",
                    operation_id
                )));
            }

            // Record exists but status wasn't in_progress - return None to indicate no update
            return Ok(None);
        }

        // Fetch the updated record
        let record = Entity::find_by_id(operation_id)
            .one(self.conn.as_ref())
            .await?;

        Ok(record)
    }

    /// Initialize progress tracking fields (total_peers, min_ack_responses)
    pub async fn initialize_progress(
        &self,
        operation_id: Uuid,
        total_peers: u16,
        min_ack_responses: u16,
    ) -> Result<Model, RepositoryError> {
        let existing = Entity::find_by_id(operation_id)
            .one(self.conn.as_ref())
            .await?
            .ok_or_else(|| {
                RepositoryError::NotFound(format!("Operation {} not found", operation_id))
            })?;

        let mut active_model: operations::ActiveModel = existing.into();
        active_model.total_peers = Set(Some(total_peers));
        active_model.min_ack_responses = Set(Some(min_ack_responses));
        active_model.completed_count = Set(0);
        active_model.failed_count = Set(0);
        active_model.updated_at = Set(Utc::now());

        let result = active_model.update(self.conn.as_ref()).await?;
        Ok(result)
    }

    /// Update progress and status together (for completion/failure)
    pub async fn update_progress_and_status(
        &self,
        operation_id: Uuid,
        status: &str,
        error_message: Option<String>,
        completed_count: u16,
        failed_count: u16,
    ) -> Result<Model, RepositoryError> {
        let existing = Entity::find_by_id(operation_id)
            .one(self.conn.as_ref())
            .await?
            .ok_or_else(|| {
                RepositoryError::NotFound(format!("Operation {} not found", operation_id))
            })?;

        let mut active_model: operations::ActiveModel = existing.into();
        active_model.status = Set(status.to_string());
        active_model.completed_count = Set(completed_count);
        active_model.failed_count = Set(failed_count);
        if let Some(em) = error_message {
            active_model.error_message = Set(Some(em));
        }
        active_model.updated_at = Set(Utc::now());

        let result = active_model.update(self.conn.as_ref()).await?;
        Ok(result)
    }

    /// Delete an operation record by ID
    pub async fn delete(&self, operation_id: Uuid) -> Result<bool, RepositoryError> {
        let result = Entity::delete_by_id(operation_id)
            .exec(self.conn.as_ref())
            .await?;

        Ok(result.rows_affected > 0)
    }

    /// Remove operations by timestamp and statuses (cleanup old records)
    pub async fn remove_by_timestamp_and_statuses(
        &self,
        timestamp: i64,
        statuses: Vec<String>,
    ) -> Result<u64, RepositoryError> {
        let result = Entity::delete_many()
            .filter(Column::Timestamp.lt(timestamp))
            .filter(Column::Status.is_in(statuses))
            .exec(self.conn.as_ref())
            .await?;

        Ok(result.rows_affected)
    }

    /// Find operations older than a timestamp, ordered by creation date
    pub async fn find_by_created_at(
        &self,
        timestamp: chrono::DateTime<Utc>,
        limit: Option<u64>,
    ) -> Result<Vec<Model>, RepositoryError> {
        let mut query = Entity::find()
            .filter(Column::CreatedAt.lte(timestamp))
            .order_by_asc(Column::CreatedAt);

        if let Some(l) = limit {
            query = query.limit(l);
        }

        let records = query.all(self.conn.as_ref()).await?;

        Ok(records)
    }

    /// Remove operations by created_at timestamp (for cleanup)
    /// Note: limit parameter is not supported by SeaORM's delete_many
    pub async fn remove_by_created_at(
        &self,
        timestamp: chrono::DateTime<Utc>,
        limit: Option<u64>,
    ) -> Result<u64, RepositoryError> {
        // If limit is specified, first find the IDs to delete, then delete them
        if let Some(l) = limit {
            let records = Entity::find()
                .filter(Column::CreatedAt.lte(timestamp))
                .order_by_asc(Column::CreatedAt)
                .limit(l)
                .all(self.conn.as_ref())
                .await?;

            let ids: Vec<Uuid> = records.into_iter().map(|r| r.operation_id).collect();

            if ids.is_empty() {
                return Ok(0);
            }

            let result = Entity::delete_many()
                .filter(Column::OperationId.is_in(ids))
                .exec(self.conn.as_ref())
                .await?;

            Ok(result.rows_affected)
        } else {
            // No limit, delete all matching records
            let result = Entity::delete_many()
                .filter(Column::CreatedAt.lte(timestamp))
                .exec(self.conn.as_ref())
                .await?;

            Ok(result.rows_affected)
        }
    }

    /// Get all operations by operation_name
    pub async fn find_by_operation_name(
        &self,
        operation_name: &str,
    ) -> Result<Vec<Model>, RepositoryError> {
        let records = Entity::find()
            .filter(Column::OperationName.eq(operation_name))
            .all(self.conn.as_ref())
            .await?;

        Ok(records)
    }

    /// Get operations by operation_name and status
    pub async fn find_by_operation_name_and_status(
        &self,
        operation_name: &str,
        status: &str,
    ) -> Result<Vec<Model>, RepositoryError> {
        let records = Entity::find()
            .filter(Column::OperationName.eq(operation_name))
            .filter(Column::Status.eq(status))
            .all(self.conn.as_ref())
            .await?;

        Ok(records)
    }

    /// Get operations by status (for filtering completed/failed operations)
    pub async fn find_by_status(&self, status: &str) -> Result<Vec<Model>, RepositoryError> {
        let records = Entity::find()
            .filter(Column::Status.eq(status))
            .all(self.conn.as_ref())
            .await?;

        Ok(records)
    }
}
