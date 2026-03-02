use std::{sync::Arc, time::Instant};

use chrono::Utc;
use sea_orm::{
    ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Statement, Value, sea_query::Expr,
};

use crate::{
    error::{RepositoryError, Result},
    models::kc_projection_state::{
        ActiveModel as ProjectionActiveModel, Column as ProjectionColumn,
        Entity as ProjectionEntity,
    },
    observability::record_repository_query,
    types::{KcProjectionActualState, KcProjectionDesiredState},
};

#[derive(Clone)]
pub struct KcProjectionRepository {
    conn: Arc<DatabaseConnection>,
}

impl KcProjectionRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    pub async fn ensure_desired_present(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<()> {
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query(
                "kc_projection",
                "ensure_desired_present",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(());
        }

        let now = Utc::now().timestamp();
        let models: Vec<ProjectionActiveModel> = kc_ids
            .iter()
            .map(|&kc_id| ProjectionActiveModel {
                blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
                contract_address: ActiveValue::Set(contract_address.to_string()),
                kc_id: ActiveValue::Set(kc_id),
                desired_state: ActiveValue::Set(KcProjectionDesiredState::Present.as_u8()),
                actual_state: ActiveValue::Set(KcProjectionActualState::Unknown.as_u8()),
                last_synced_at: ActiveValue::Set(None),
                last_error: ActiveValue::Set(None),
                created_at: ActiveValue::Set(now),
                updated_at: ActiveValue::Set(now),
            })
            .collect();

        let result = ProjectionEntity::insert_many(models)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([
                    ProjectionColumn::BlockchainId,
                    ProjectionColumn::ContractAddress,
                    ProjectionColumn::KcId,
                ])
                .update_columns([ProjectionColumn::DesiredState, ProjectionColumn::UpdatedAt])
                .to_owned(),
            )
            .exec(self.conn.as_ref())
            .await
            .map(|_| ())
            .map_err(Into::into);

        match &result {
            Ok(()) => {
                record_repository_query(
                    "kc_projection",
                    "ensure_desired_present",
                    "ok",
                    started.elapsed(),
                    Some(kc_ids.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_projection",
                    "ensure_desired_present",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    pub async fn mark_present(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<()> {
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query(
                "kc_projection",
                "mark_present",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(());
        }

        let now = Utc::now().timestamp();
        let result = ProjectionEntity::update_many()
            .col_expr(
                ProjectionColumn::ActualState,
                Expr::value(KcProjectionActualState::Present.as_u8()),
            )
            .col_expr(ProjectionColumn::LastSyncedAt, Expr::value(Some(now)))
            .col_expr(ProjectionColumn::LastError, Expr::value(None::<String>))
            .col_expr(ProjectionColumn::UpdatedAt, Expr::value(now))
            .filter(ProjectionColumn::BlockchainId.eq(blockchain_id))
            .filter(ProjectionColumn::ContractAddress.eq(contract_address))
            .filter(ProjectionColumn::KcId.is_in(kc_ids.to_vec()))
            .exec(self.conn.as_ref())
            .await
            .map(|_| ())
            .map_err(Into::into);

        match &result {
            Ok(()) => {
                record_repository_query(
                    "kc_projection",
                    "mark_present",
                    "ok",
                    started.elapsed(),
                    Some(kc_ids.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_projection",
                    "mark_present",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    pub async fn mark_pending(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<()> {
        self.mark_pending_with_error(blockchain_id, contract_address, kc_ids, None)
            .await
    }

    pub async fn mark_pending_with_error(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
        error_reason: Option<&str>,
    ) -> Result<()> {
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query(
                "kc_projection",
                "mark_pending_with_error",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(());
        }

        let now = Utc::now().timestamp();
        let result = ProjectionEntity::update_many()
            .col_expr(
                ProjectionColumn::ActualState,
                Expr::value(KcProjectionActualState::Pending.as_u8()),
            )
            .col_expr(
                ProjectionColumn::LastError,
                Expr::value(error_reason.map(ToString::to_string)),
            )
            .col_expr(ProjectionColumn::UpdatedAt, Expr::value(now))
            .filter(ProjectionColumn::BlockchainId.eq(blockchain_id))
            .filter(ProjectionColumn::ContractAddress.eq(contract_address))
            // Do not downgrade already-materialized rows back to pending.
            .filter(ProjectionColumn::ActualState.ne(KcProjectionActualState::Present.as_u8()))
            .filter(ProjectionColumn::KcId.is_in(kc_ids.to_vec()))
            .exec(self.conn.as_ref())
            .await
            .map(|_| ())
            .map_err(Into::into);

        match &result {
            Ok(()) => {
                record_repository_query(
                    "kc_projection",
                    "mark_pending_with_error",
                    "ok",
                    started.elapsed(),
                    Some(kc_ids.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_projection",
                    "mark_pending_with_error",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    pub async fn mark_failed(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
        error_reason: &str,
    ) -> Result<()> {
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query(
                "kc_projection",
                "mark_failed",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(());
        }

        let now = Utc::now().timestamp();
        let result = ProjectionEntity::update_many()
            .col_expr(
                ProjectionColumn::ActualState,
                Expr::value(KcProjectionActualState::Failed.as_u8()),
            )
            .col_expr(
                ProjectionColumn::LastError,
                Expr::value(Some(error_reason.to_string())),
            )
            .col_expr(ProjectionColumn::UpdatedAt, Expr::value(now))
            .filter(ProjectionColumn::BlockchainId.eq(blockchain_id))
            .filter(ProjectionColumn::ContractAddress.eq(contract_address))
            .filter(ProjectionColumn::KcId.is_in(kc_ids.to_vec()))
            .exec(self.conn.as_ref())
            .await
            .map(|_| ())
            .map_err(Into::into);

        match &result {
            Ok(()) => {
                record_repository_query(
                    "kc_projection",
                    "mark_failed",
                    "ok",
                    started.elapsed(),
                    Some(kc_ids.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_projection",
                    "mark_failed",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    pub async fn mark_unknown_from_pending(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<()> {
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query(
                "kc_projection",
                "mark_unknown_from_pending",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(());
        }

        let now = Utc::now().timestamp();
        let result = ProjectionEntity::update_many()
            .col_expr(
                ProjectionColumn::ActualState,
                Expr::value(KcProjectionActualState::Unknown.as_u8()),
            )
            .col_expr(ProjectionColumn::LastError, Expr::value(None::<String>))
            .col_expr(ProjectionColumn::UpdatedAt, Expr::value(now))
            .filter(ProjectionColumn::BlockchainId.eq(blockchain_id))
            .filter(ProjectionColumn::ContractAddress.eq(contract_address))
            .filter(ProjectionColumn::ActualState.eq(KcProjectionActualState::Pending.as_u8()))
            .filter(ProjectionColumn::KcId.is_in(kc_ids.to_vec()))
            .exec(self.conn.as_ref())
            .await
            .map(|_| ())
            .map_err(Into::into);

        match &result {
            Ok(()) => {
                record_repository_query(
                    "kc_projection",
                    "mark_unknown_from_pending",
                    "ok",
                    started.elapsed(),
                    Some(kc_ids.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_projection",
                    "mark_unknown_from_pending",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Return projection keys that should be reconciled against triple-store materialization.
    ///
    /// Includes rows with desired=Present and actual=Unknown.
    /// Failed rows are terminal and require explicit operator reset.
    pub async fn list_non_present_desired_keys(
        &self,
        blockchain_id: &str,
        limit: usize,
    ) -> Result<Vec<(String, u64)>> {
        let started = Instant::now();
        if limit == 0 {
            record_repository_query(
                "kc_projection",
                "list_non_present_desired_keys",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(Vec::new());
        }

        let result = ProjectionEntity::find()
            .filter(ProjectionColumn::BlockchainId.eq(blockchain_id))
            .filter(ProjectionColumn::DesiredState.eq(KcProjectionDesiredState::Present.as_u8()))
            .filter(ProjectionColumn::ActualState.eq(KcProjectionActualState::Unknown.as_u8()))
            .order_by_asc(ProjectionColumn::UpdatedAt)
            .order_by_asc(ProjectionColumn::ContractAddress)
            .order_by_asc(ProjectionColumn::KcId)
            .limit(limit as u64)
            .all(self.conn.as_ref())
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| (row.contract_address, row.kc_id))
                    .collect::<Vec<_>>()
            })
            .map_err(Into::into);

        match &result {
            Ok(rows) => {
                record_repository_query(
                    "kc_projection",
                    "list_non_present_desired_keys",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_projection",
                    "list_non_present_desired_keys",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Return pending projection keys that do not have a matching queue row.
    pub async fn list_pending_keys_missing_queue(
        &self,
        blockchain_id: &str,
        limit: usize,
    ) -> Result<Vec<(String, u64)>> {
        let started = Instant::now();
        if limit == 0 {
            record_repository_query(
                "kc_projection",
                "list_pending_keys_missing_queue",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(Vec::new());
        }

        let db = self.conn.as_ref();
        let limit_i64 = Self::u64_to_i64(limit as u64, "limit")?;
        let sql = Statement::from_sql_and_values(
            db.get_database_backend(),
            r#"
            SELECT p.contract_address, p.kc_id
            FROM kc_projection_state p
            LEFT JOIN kc_sync_queue q
                ON q.blockchain_id = p.blockchain_id
               AND q.contract_address = p.contract_address
               AND q.kc_id = p.kc_id
            WHERE p.blockchain_id = ?
              AND p.desired_state = ?
              AND p.actual_state = ?
              AND q.kc_id IS NULL
            ORDER BY p.updated_at ASC, p.contract_address ASC, p.kc_id ASC
            LIMIT ?
            "#,
            [
                Value::String(Some(Box::new(blockchain_id.to_string()))),
                Value::TinyUnsigned(Some(KcProjectionDesiredState::Present.as_u8())),
                Value::TinyUnsigned(Some(KcProjectionActualState::Pending.as_u8())),
                Value::BigInt(Some(limit_i64)),
            ],
        );

        let result = async {
            let rows = db.query_all(sql).await.map_err(RepositoryError::Database)?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let contract_address: String = row
                    .try_get("", "contract_address")
                    .map_err(RepositoryError::Database)?;
                let kc_id: u64 = row
                    .try_get("", "kc_id")
                    .map_err(RepositoryError::Database)?;
                out.push((contract_address, kc_id));
            }
            Ok(out)
        }
        .await;

        match &result {
            Ok(rows) => {
                record_repository_query(
                    "kc_projection",
                    "list_pending_keys_missing_queue",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_projection",
                    "list_pending_keys_missing_queue",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    fn u64_to_i64(value: u64, field: &'static str) -> Result<i64> {
        i64::try_from(value).map_err(|_| {
            RepositoryError::SyncMetadata(format!("{field} value {value} exceeds i64 range"))
        })
    }
}
