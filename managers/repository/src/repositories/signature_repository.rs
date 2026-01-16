use std::sync::Arc;

use chrono::Utc;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, Set};
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

    #[allow(clippy::too_many_arguments)]
    /// Add a signature for an operation
    async fn create(
        &self,
        operation_id: Uuid,
        is_publisher: bool,
        identity_id: &str,
        v: u8,
        r: &str,
        s: &str,
        vs: &str,
    ) -> Result<Model, RepositoryError> {
        let now = Utc::now();

        let active_model = signatures::ActiveModel {
            id: Set(0), // Auto-increment
            operation_id: Set(operation_id),
            is_publisher: Set(is_publisher),
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

    pub async fn store_publisher_signature(
        &self,
        operation_id: Uuid,
        identity_id: &str,
        v: u8,
        r: &str,
        s: &str,
        vs: &str,
    ) -> Result<Model, RepositoryError> {
        self.create(operation_id, true, identity_id, v, r, s, vs)
            .await
    }

    pub async fn store_network_signature(
        &self,
        operation_id: Uuid,
        identity_id: &str,
        v: u8,
        r: &str,
        s: &str,
        vs: &str,
    ) -> Result<Model, RepositoryError> {
        self.create(operation_id, false, identity_id, v, r, s, vs)
            .await
    }

    /// Get all signatures for an operation by type
    pub async fn get_publisher_signature(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<Model>, RepositoryError> {
        let signature = Entity::find()
            .filter(Column::OperationId.eq(operation_id))
            .filter(Column::IsPublisher.eq(true))
            .one(self.conn.as_ref())
            .await?;

        Ok(signature)
    }

    /// Get all signatures for an operation by type
    pub async fn get_network_signatures(
        &self,
        operation_id: Uuid,
    ) -> Result<Vec<Model>, RepositoryError> {
        let signatures = Entity::find()
            .filter(Column::OperationId.eq(operation_id))
            .filter(Column::IsPublisher.eq(false))
            .all(self.conn.as_ref())
            .await?;

        Ok(signatures)
    }

    /// Get all signatures for an operation (regardless of type)
    pub async fn get_by_operation(
        &self,
        operation_id: Uuid,
    ) -> Result<Vec<Model>, RepositoryError> {
        let signatures = Entity::find()
            .filter(Column::OperationId.eq(operation_id))
            .all(self.conn.as_ref())
            .await?;

        Ok(signatures)
    }

    /// Delete all signatures for an operation (cleanup)
    pub async fn delete_by_operation(&self, operation_id: Uuid) -> Result<u64, RepositoryError> {
        let result = Entity::delete_many()
            .filter(Column::OperationId.eq(operation_id))
            .exec(self.conn.as_ref())
            .await?;

        Ok(result.rows_affected)
    }
}
