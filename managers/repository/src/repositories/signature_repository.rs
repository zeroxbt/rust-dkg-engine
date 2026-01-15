use std::sync::Arc;

use chrono::Utc;
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
use uuid::Uuid;

use crate::{
    error::RepositoryError,
    models::signatures::{self, Column, Entity, Model},
};

pub struct SignatureRepository {
    conn: Arc<DatabaseConnection>,
}

impl SignatureRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Add a signature for an operation
    pub async fn create(
        &self,
        operation_id: Uuid,
        signature_type: &str,
        identity_id: &str,
        v: u8,
        r: &str,
        s: &str,
        vs: &str,
    ) -> Result<Model, RepositoryError> {
        let now = Utc::now();

        let active_model = signatures::ActiveModel {
            id: Set(0), // Auto-increment
            operation_id: Set(operation_id.to_string()),
            signature_type: Set(signature_type.to_string()),
            identity_id: Set(identity_id.to_string()),
            v: Set(v),
            r: Set(r.to_string()),
            s: Set(s.to_string()),
            vs: Set(vs.to_string()),
            created_at: Set(now),
            updated_at: Set(now),
        };

        let result = Entity::insert(active_model)
            .exec_with_returning(self.conn.as_ref())
            .await?;

        Ok(result)
    }

    /// Get all signatures for an operation by type
    pub async fn get_by_operation_and_type(
        &self,
        operation_id: &str,
        signature_type: &str,
    ) -> Result<Vec<Model>, RepositoryError> {
        let signatures = Entity::find()
            .filter(Column::OperationId.eq(operation_id))
            .filter(Column::SignatureType.eq(signature_type))
            .all(self.conn.as_ref())
            .await?;

        Ok(signatures)
    }

    /// Get all signatures for an operation (regardless of type)
    pub async fn get_by_operation(
        &self,
        operation_id: &str,
    ) -> Result<Vec<Model>, RepositoryError> {
        let signatures = Entity::find()
            .filter(Column::OperationId.eq(operation_id))
            .all(self.conn.as_ref())
            .await?;

        Ok(signatures)
    }

    /// Delete all signatures for an operation (cleanup)
    pub async fn delete_by_operation(&self, operation_id: &str) -> Result<u64, RepositoryError> {
        let result = Entity::delete_many()
            .filter(Column::OperationId.eq(operation_id))
            .exec(self.conn.as_ref())
            .await?;

        Ok(result.rows_affected)
    }
}
