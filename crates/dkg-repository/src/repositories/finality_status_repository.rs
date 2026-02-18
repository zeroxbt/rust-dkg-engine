use std::sync::Arc;

use chrono::Utc;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
};
use uuid::Uuid;

use crate::{
    error::RepositoryError,
    models::finality_status::{self, Column, Entity},
};

#[derive(Clone)]
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
    ) -> Result<(), RepositoryError> {
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

            Entity::update(active_model)
                .exec(self.conn.as_ref())
                .await?;
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

            Entity::insert(active_model)
                .exec_without_returning(self.conn.as_ref())
                .await?;
        }

        Ok(())
    }

    /// Get the count of finality acks for a given UAL.
    pub async fn get_finality_acks_count(&self, ual: &str) -> Result<u64, RepositoryError> {
        let count = Entity::find()
            .filter(Column::Ual.eq(ual))
            .count(self.conn.as_ref())
            .await?;

        Ok(count)
    }

    /// Find finality ack IDs older than the cutoff time.
    pub async fn find_ids_older_than(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        limit: u64,
    ) -> Result<Vec<i32>, RepositoryError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let records = Entity::find()
            .filter(Column::UpdatedAt.lt(cutoff))
            .order_by_asc(Column::UpdatedAt)
            .limit(limit)
            .all(self.conn.as_ref())
            .await?;

        Ok(records.into_iter().map(|record| record.id).collect())
    }

    /// Delete finality ack records by ID. Returns rows affected.
    pub async fn delete_by_ids(&self, ids: &[i32]) -> Result<u64, RepositoryError> {
        if ids.is_empty() {
            return Ok(0);
        }

        let result = Entity::delete_many()
            .filter(Column::Id.is_in(ids.to_vec()))
            .exec(self.conn.as_ref())
            .await?;

        Ok(result.rows_affected)
    }
}
