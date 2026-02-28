use std::{sync::Arc, time::Instant};

use chrono::Utc;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
};
use uuid::Uuid;

use crate::{
    error::RepositoryError,
    models::operations::{self, Entity, Model},
    observability::record_repository_query,
    types::{OperationRecord, OperationStatus},
};

#[derive(Clone)]
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
        let started = Instant::now();
        let now = Utc::now();

        let active_model = operations::ActiveModel {
            operation_id: Set(operation_id.to_string()),
            operation_name: Set(operation_name.to_string()),
            status: Set(status.as_str().to_string()),
            error_message: Set(None),
            created_at: Set(now),
            updated_at: Set(now),
        };

        let result = Entity::insert(active_model)
            .exec_without_returning(self.conn.as_ref())
            .await
            .map(|_| ())
            .map_err(Into::into);

        match &result {
            Ok(()) => {
                record_repository_query("operation", "create", "ok", started.elapsed(), Some(1));
            }
            Err(_) => {
                record_repository_query("operation", "create", "error", started.elapsed(), None);
            }
        }

        result
    }

    /// Get an operation record by its ID and name
    pub async fn get_by_id_and_name(
        &self,
        operation_id: Uuid,
        operation_name: &str,
    ) -> Result<Option<OperationRecord>, RepositoryError> {
        let started = Instant::now();
        let result = Entity::find()
            .filter(operations::Column::OperationId.eq(operation_id.to_string()))
            .filter(operations::Column::OperationName.eq(operation_name))
            .one(self.conn.as_ref())
            .await
            .map(|record| record.map(Self::to_record));

        match &result {
            Ok(Some(_)) => {
                record_repository_query(
                    "operation",
                    "get_by_id_and_name",
                    "ok",
                    started.elapsed(),
                    Some(1),
                );
            }
            Ok(None) => {
                record_repository_query(
                    "operation",
                    "get_by_id_and_name",
                    "ok",
                    started.elapsed(),
                    Some(0),
                );
            }
            Err(_) => {
                record_repository_query(
                    "operation",
                    "get_by_id_and_name",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result.map_err(Into::into)
    }

    /// Get operation created_at timestamp (milliseconds since epoch) by operation id.
    pub async fn get_created_at_timestamp_millis(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<i64>, RepositoryError> {
        let started = Instant::now();
        let result = Entity::find_by_id(operation_id.to_string())
            .one(self.conn.as_ref())
            .await
            .map(|record| record.map(|value| value.created_at.timestamp_millis()));

        match &result {
            Ok(Some(_)) => {
                record_repository_query(
                    "operation",
                    "get_created_at_timestamp_millis",
                    "ok",
                    started.elapsed(),
                    Some(1),
                );
            }
            Ok(None) => {
                record_repository_query(
                    "operation",
                    "get_created_at_timestamp_millis",
                    "ok",
                    started.elapsed(),
                    Some(0),
                );
            }
            Err(_) => {
                record_repository_query(
                    "operation",
                    "get_created_at_timestamp_millis",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result.map_err(Into::into)
    }

    /// Update an operation record with flexible field updates
    pub async fn update(
        &self,
        operation_id: Uuid,
        status: Option<OperationStatus>,
        error_message: Option<String>,
    ) -> Result<(), RepositoryError> {
        let started = Instant::now();
        // First, find the existing record
        let result = async {
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
        .await;

        match &result {
            Ok(()) => {
                record_repository_query("operation", "update", "ok", started.elapsed(), Some(1));
            }
            Err(_) => {
                record_repository_query("operation", "update", "error", started.elapsed(), None);
            }
        }

        result
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
        let started = Instant::now();
        if statuses.is_empty() || limit == 0 {
            record_repository_query(
                "operation",
                "find_ids_by_status_older_than",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(Vec::new());
        }

        let status_values: Vec<String> = statuses
            .iter()
            .map(|status| status.as_str().to_string())
            .collect();

        let result = Entity::find()
            .filter(operations::Column::Status.is_in(status_values))
            .filter(operations::Column::UpdatedAt.lt(cutoff))
            .order_by_asc(operations::Column::UpdatedAt)
            .limit(limit)
            .all(self.conn.as_ref())
            .await
            .map(|records| {
                let mut ids = Vec::with_capacity(records.len());
                for record in records {
                    if let Ok(id) = Uuid::parse_str(&record.operation_id) {
                        ids.push(id);
                    }
                }
                ids
            });

        match &result {
            Ok(ids) => {
                record_repository_query(
                    "operation",
                    "find_ids_by_status_older_than",
                    "ok",
                    started.elapsed(),
                    Some(ids.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "operation",
                    "find_ids_by_status_older_than",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result.map_err(Into::into)
    }

    /// Delete operations by ID. Returns rows affected.
    pub async fn delete_by_ids(&self, ids: &[Uuid]) -> Result<u64, RepositoryError> {
        let started = Instant::now();
        if ids.is_empty() {
            record_repository_query(
                "operation",
                "delete_by_ids",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(0);
        }

        let id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();

        let result = Entity::delete_many()
            .filter(operations::Column::OperationId.is_in(id_strings))
            .exec(self.conn.as_ref())
            .await?;

        record_repository_query(
            "operation",
            "delete_by_ids",
            "ok",
            started.elapsed(),
            Some(result.rows_affected as usize),
        );

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
