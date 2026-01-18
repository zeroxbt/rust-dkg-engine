use std::sync::Arc;

use chrono::Utc;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, Set};
use uuid::Uuid;

use crate::{
    error::RepositoryError,
    models::finality_status::{self, Column, Entity, Model},
};

pub struct FinalityStatusRepository {
    conn: Arc<DatabaseConnection>,
}

impl FinalityStatusRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Save a finality ack from a peer for a given UAL.
    /// Uses upsert semantics - if the (ual, peer_id) combination already exists, it updates.
    pub async fn save_finality_ack(
        &self,
        operation_id: Uuid,
        ual: &str,
        peer_id: &str,
    ) -> Result<Model, RepositoryError> {
        let now = Utc::now();

        // Check if record already exists
        let existing = Entity::find()
            .filter(Column::Ual.eq(ual))
            .filter(Column::PeerId.eq(peer_id))
            .one(self.conn.as_ref())
            .await?;

        if let Some(existing_model) = existing {
            // Update the existing record
            let active_model = finality_status::ActiveModel {
                id: Set(existing_model.id),
                operation_id: Set(operation_id.to_string()),
                ual: Set(ual.to_string()),
                peer_id: Set(peer_id.to_string()),
                created_at: Set(existing_model.created_at),
                updated_at: Set(now),
            };

            let result = Entity::update(active_model)
                .exec(self.conn.as_ref())
                .await?;

            Ok(result)
        } else {
            // Insert new record
            let active_model = finality_status::ActiveModel {
                id: Set(0), // Auto-increment
                operation_id: Set(operation_id.to_string()),
                ual: Set(ual.to_string()),
                peer_id: Set(peer_id.to_string()),
                created_at: Set(now),
                updated_at: Set(now),
            };

            let result = Entity::insert(active_model)
                .exec_with_returning(self.conn.as_ref())
                .await?;

            Ok(result)
        }
    }

    /// Get the count of finality acks for a given UAL.
    pub async fn get_finality_acks_count(&self, ual: &str) -> Result<u64, RepositoryError> {
        let count = Entity::find()
            .filter(Column::Ual.eq(ual))
            .count(self.conn.as_ref())
            .await?;

        Ok(count)
    }

    /// Get all finality acks for a given UAL.
    pub async fn get_finality_acks_by_ual(&self, ual: &str) -> Result<Vec<Model>, RepositoryError> {
        let acks = Entity::find()
            .filter(Column::Ual.eq(ual))
            .all(self.conn.as_ref())
            .await?;

        Ok(acks)
    }

    /// Get all finality acks for a given operation.
    pub async fn get_finality_acks_by_operation(
        &self,
        operation_id: Uuid,
    ) -> Result<Vec<Model>, RepositoryError> {
        let acks = Entity::find()
            .filter(Column::OperationId.eq(operation_id.to_string()))
            .all(self.conn.as_ref())
            .await?;

        Ok(acks)
    }

    /// Check if a peer has already sent a finality ack for a given UAL.
    pub async fn has_finality_ack(
        &self,
        ual: &str,
        peer_id: &str,
    ) -> Result<bool, RepositoryError> {
        let existing = Entity::find()
            .filter(Column::Ual.eq(ual))
            .filter(Column::PeerId.eq(peer_id))
            .one(self.conn.as_ref())
            .await?;

        Ok(existing.is_some())
    }
}
