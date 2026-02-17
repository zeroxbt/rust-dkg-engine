use std::sync::Arc;

use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
};
use uuid::Uuid;

use crate::{
    error::RepositoryError,
    models::operations::{self, Entity, Model},
    types::{OperationRecord, OperationStatus},
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
        status: OperationStatus,
    ) -> Result<(), RepositoryError> {
        let now = Utc::now();

        let active_model = operations::ActiveModel {
            operation_id: Set(operation_id.to_string()),
            operation_name: Set(operation_name.to_string()),
            status: Set(status.as_str().to_string()),
            error_message: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        };

        Entity::insert(active_model)
            .exec_without_returning(self.conn.as_ref())
            .await?;

        Ok(())
    }

    /// Get an operation record by its ID and name
    pub async fn get_by_id_and_name(
        &self,
        operation_id: Uuid,
        operation_name: &str,
    ) -> Result<Option<OperationRecord>, RepositoryError> {
        let record = Entity::find()
            .filter(operations::Column::OperationId.eq(operation_id.to_string()))
            .filter(operations::Column::OperationName.eq(operation_name))
            .one(self.conn.as_ref())
            .await?;

        Ok(record.map(Self::to_record))
    }

    /// Update an operation record with flexible field updates
    pub async fn update(
        &self,
        operation_id: Uuid,
        status: Option<OperationStatus>,
        error_message: Option<String>,
    ) -> Result<(), RepositoryError> {
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

        active_model.updated_at = Set(Utc::now());

        active_model.update(self.conn.as_ref()).await?;
        Ok(())
    }

    /// Update only the status field
    pub async fn update_status(
        &self,
        operation_id: Uuid,
        status: OperationStatus,
    ) -> Result<(), RepositoryError> {
        self.update(operation_id, Some(status), None).await
    }

    /// Find operation IDs by status and updated_at cutoff, limited by `limit`.
    pub async fn find_ids_by_status_older_than(
        &self,
        statuses: &[OperationStatus],
        cutoff: chrono::DateTime<chrono::Utc>,
        limit: u64,
    ) -> Result<Vec<Uuid>, RepositoryError> {
        if statuses.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }

        let status_values: Vec<String> = statuses
            .iter()
            .map(|status| status.as_str().to_string())
            .collect();

        let records = Entity::find()
            .filter(operations::Column::Status.is_in(status_values))
            .filter(operations::Column::UpdatedAt.lt(cutoff))
            .order_by_asc(operations::Column::UpdatedAt)
            .limit(limit)
            .all(self.conn.as_ref())
            .await?;

        let mut ids = Vec::with_capacity(records.len());
        for record in records {
            if let Ok(id) = Uuid::parse_str(&record.operation_id) {
                ids.push(id);
            }
        }

        Ok(ids)
    }

    /// Delete operations by ID. Returns rows affected.
    pub async fn delete_by_ids(&self, ids: &[Uuid]) -> Result<u64, RepositoryError> {
        if ids.is_empty() {
            return Ok(0);
        }

        let id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();

        let result = Entity::delete_many()
            .filter(operations::Column::OperationId.is_in(id_strings))
            .exec(self.conn.as_ref())
            .await?;

        Ok(result.rows_affected)
    }

    fn to_record(model: Model) -> OperationRecord {
        OperationRecord {
            operation_id: model.operation_id,
            operation_name: model.operation_name,
            status: model.status,
            error_message: model.error_message,
        }
    }
}
