use crate::error::RepositoryError;
use crate::models::operation_ids::{self, Entity, Model};
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use std::sync::Arc;
use uuid::Uuid;

pub struct OperationIdRepository {
    conn: Arc<DatabaseConnection>,
}

impl OperationIdRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Create a new operation ID record
    pub async fn create(
        &self,
        operation_id: Uuid,
        status: &str,
        timestamp: i64,
    ) -> Result<Model, RepositoryError> {
        let now = chrono::Utc::now();

        let active_model = operation_ids::ActiveModel {
            operation_id: Set(operation_id),
            status: Set(status.to_string()),
            data: Set(None),
            timestamp: Set(timestamp),
            created_at: Set(now),
            updated_at: Set(now),
        };

        let result = Entity::insert(active_model)
            .exec_with_returning(self.conn.as_ref())
            .await?;

        Ok(result)
    }

    /// Get an operation ID record by its ID
    pub async fn get(&self, operation_id: Uuid) -> Result<Option<Model>, RepositoryError> {
        let record = Entity::find_by_id(operation_id)
            .one(self.conn.as_ref())
            .await?;

        Ok(record)
    }

    /// Update an operation ID record
    pub async fn update(
        &self,
        operation_id: Uuid,
        status: Option<&str>,
        data: Option<String>,
        timestamp: Option<i64>,
    ) -> Result<Model, RepositoryError> {
        // First, find the existing record
        let existing = Entity::find_by_id(operation_id)
            .one(self.conn.as_ref())
            .await?
            .ok_or_else(|| {
                RepositoryError::NotFound(format!("Operation ID {} not found", operation_id))
            })?;

        let mut active_model: operation_ids::ActiveModel = existing.into();

        // Update fields if provided
        if let Some(s) = status {
            active_model.status = Set(s.to_string());
        }
        if let Some(d) = data {
            active_model.data = Set(Some(d));
        }
        if let Some(ts) = timestamp {
            active_model.timestamp = Set(ts);
        }

        active_model.updated_at = Set(chrono::Utc::now());

        let result = active_model.update(self.conn.as_ref()).await?;

        Ok(result)
    }

    /// Delete an operation ID record
    pub async fn delete(&self, operation_id: Uuid) -> Result<bool, RepositoryError> {
        let result = Entity::delete_by_id(operation_id)
            .exec(self.conn.as_ref())
            .await?;

        Ok(result.rows_affected > 0)
    }
}
