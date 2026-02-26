use std::{sync::Arc, time::Instant};

use chrono::Utc;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
};
use uuid::Uuid;

use crate::{
    error::RepositoryError,
    models::finality_status::{self, Column, Entity},
    observability::record_repository_query,
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
        let started = Instant::now();
        let now = Utc::now();

        let result = async {
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

                Entity::update(active_model).exec(self.conn.as_ref()).await?;
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
        .await;

        match &result {
            Ok(()) => {
                record_repository_query("finality_status", "save_finality_ack", "ok", started.elapsed(), Some(1));
            }
            Err(_) => {
                record_repository_query("finality_status", "save_finality_ack", "error", started.elapsed(), None);
            }
        }

        result
    }

    /// Get the count of finality acks for a given UAL.
    pub async fn get_finality_acks_count(&self, ual: &str) -> Result<u64, RepositoryError> {
        let started = Instant::now();
        let result = Entity::find()
            .filter(Column::Ual.eq(ual))
            .count(self.conn.as_ref())
            .await;

        match &result {
            Ok(count) => {
                record_repository_query(
                    "finality_status",
                    "get_finality_acks_count",
                    "ok",
                    started.elapsed(),
                    Some(*count as usize),
                );
            }
            Err(_) => {
                record_repository_query(
                    "finality_status",
                    "get_finality_acks_count",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result.map_err(Into::into)
    }

    /// Find finality ack IDs older than the cutoff time.
    pub async fn find_ids_older_than(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        limit: u64,
    ) -> Result<Vec<i32>, RepositoryError> {
        let started = Instant::now();
        if limit == 0 {
            record_repository_query("finality_status", "find_ids_older_than", "ok", started.elapsed(), Some(0));
            return Ok(Vec::new());
        }

        let result = Entity::find()
            .filter(Column::UpdatedAt.lt(cutoff))
            .order_by_asc(Column::UpdatedAt)
            .limit(limit)
            .all(self.conn.as_ref())
            .await
            .map(|records| records.into_iter().map(|record| record.id).collect::<Vec<_>>());

        match &result {
            Ok(rows) => {
                record_repository_query("finality_status", "find_ids_older_than", "ok", started.elapsed(), Some(rows.len()));
            }
            Err(_) => {
                record_repository_query("finality_status", "find_ids_older_than", "error", started.elapsed(), None);
            }
        }

        result.map_err(Into::into)
    }

    /// Delete finality ack records by ID. Returns rows affected.
    pub async fn delete_by_ids(&self, ids: &[i32]) -> Result<u64, RepositoryError> {
        let started = Instant::now();
        if ids.is_empty() {
            record_repository_query("finality_status", "delete_by_ids", "ok", started.elapsed(), Some(0));
            return Ok(0);
        }

        let result = Entity::delete_many()
            .filter(Column::Id.is_in(ids.to_vec()))
            .exec(self.conn.as_ref())
            .await?;

        record_repository_query(
            "finality_status",
            "delete_by_ids",
            "ok",
            started.elapsed(),
            Some(result.rows_affected as usize),
        );

        Ok(result.rows_affected)
    }
}
