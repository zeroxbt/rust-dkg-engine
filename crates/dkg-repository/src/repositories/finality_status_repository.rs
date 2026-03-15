use std::{sync::Arc, time::Instant};

use chrono::Utc;
use dkg_observability::record_repository_query;
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
        let started = Instant::now();
        let now = Utc::now();

        let active_model = finality_status::ActiveModel {
            id: Set(0), // Auto-increment
            operation_id: Set(operation_id.to_string()),
            ual: Set(ual.to_string()),
            peer_id: Set(peer_id.to_string()),
            created_at: Set(now),
            updated_at: Set(now),
        };
        let result = Entity::insert(active_model)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([Column::Ual, Column::PeerId])
                    .update_columns([Column::OperationId, Column::UpdatedAt])
                    .to_owned(),
            )
            .exec(self.conn.as_ref())
            .await
            .map(|_| ());

        match &result {
            Ok(()) => {
                record_repository_query(
                    "finality_status",
                    "save_finality_ack",
                    "ok",
                    started.elapsed(),
                    Some(1),
                );
            }
            Err(_) => {
                record_repository_query(
                    "finality_status",
                    "save_finality_ack",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result.map_err(Into::into)
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
            record_repository_query(
                "finality_status",
                "find_ids_older_than",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(Vec::new());
        }

        let result = Entity::find()
            .filter(Column::UpdatedAt.lt(cutoff))
            .order_by_asc(Column::UpdatedAt)
            .limit(limit)
            .select_only()
            .column(Column::Id)
            .into_tuple::<i32>()
            .all(self.conn.as_ref())
            .await;

        match &result {
            Ok(rows) => {
                record_repository_query(
                    "finality_status",
                    "find_ids_older_than",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "finality_status",
                    "find_ids_older_than",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result.map_err(Into::into)
    }

    /// Delete finality ack records by ID. Returns rows affected.
    pub async fn delete_by_ids(&self, ids: &[i32]) -> Result<u64, RepositoryError> {
        let started = Instant::now();
        if ids.is_empty() {
            record_repository_query(
                "finality_status",
                "delete_by_ids",
                "ok",
                started.elapsed(),
                Some(0),
            );
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
