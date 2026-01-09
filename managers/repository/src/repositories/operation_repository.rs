use crate::error::RepositoryError;
use crate::models::operations::{self, Column, Entity, Model};
use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
};
use std::sync::Arc;
use uuid::Uuid;

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
            final_status: Set(None),
            request_data: Set(None),
            response_data: Set(None),
            error_message: Set(None),
            min_acks_reached: Set(false),
            timestamp: Set(timestamp),
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
        final_status: Option<&str>,
        request_data: Option<String>,
        response_data: Option<String>,
        error_message: Option<String>,
        min_acks_reached: Option<bool>,
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
        if let Some(s) = status {
            active_model.status = Set(s.to_string());
        }
        if let Some(fs) = final_status {
            active_model.final_status = Set(Some(fs.to_string()));
        }
        if let Some(rd) = request_data {
            active_model.request_data = Set(Some(rd));
        }
        if let Some(rd) = response_data {
            active_model.response_data = Set(Some(rd));
        }
        if let Some(em) = error_message {
            active_model.error_message = Set(Some(em));
        }
        if let Some(mar) = min_acks_reached {
            active_model.min_acks_reached = Set(mar);
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
        self.update(
            operation_id,
            Some(status),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
    }

    /// Update min_acks_reached flag
    pub async fn update_min_acks_reached(
        &self,
        operation_id: Uuid,
        min_acks_reached: bool,
    ) -> Result<Model, RepositoryError> {
        self.update(
            operation_id,
            None,
            None,
            None,
            None,
            None,
            Some(min_acks_reached),
            None,
        )
        .await
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

    /// Get operations by final_status (for filtering completed/failed operations)
    pub async fn find_by_final_status(
        &self,
        final_status: &str,
    ) -> Result<Vec<Model>, RepositoryError> {
        let records = Entity::find()
            .filter(Column::FinalStatus.eq(final_status))
            .all(self.conn.as_ref())
            .await?;

        Ok(records)
    }

    /// Get all in-progress operations (where final_status is NULL)
    pub async fn find_in_progress(&self) -> Result<Vec<Model>, RepositoryError> {
        let records = Entity::find()
            .filter(Column::FinalStatus.is_null())
            .all(self.conn.as_ref())
            .await?;

        Ok(records)
    }
}
