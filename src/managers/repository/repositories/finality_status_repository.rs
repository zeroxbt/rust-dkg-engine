use std::sync::Arc;

use chrono::Utc;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, Set};
use uuid::Uuid;

use crate::managers::repository::{
    error::RepositoryError,
    models::finality_status::{self, Column, Entity, Model},
};

pub(crate) struct FinalityStatusRepository {
    conn: Arc<DatabaseConnection>,
}

impl FinalityStatusRepository {
    pub(crate) fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Save a finality ack from a peer for a given UAL.
    /// Uses upsert semantics - if the (ual, peer_id) combination already exists, it updates.
    pub(crate) async fn save_finality_ack(
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
    pub(crate) async fn get_finality_acks_count(&self, ual: &str) -> Result<u64, RepositoryError> {
        let count = Entity::find()
            .filter(Column::Ual.eq(ual))
            .count(self.conn.as_ref())
            .await?;

        Ok(count)
    }
}
