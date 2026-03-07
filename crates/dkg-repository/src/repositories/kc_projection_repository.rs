use std::{sync::Arc, time::Instant};

use chrono::Utc;
use sea_orm::{
    ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect,
    sea_query::{Alias, Expr, JoinType, Order, Query},
};

use dkg_observability::record_repository_query;

use crate::{
    error::Result,
    models::kc_projection_state::{
        ActiveModel as ProjectionActiveModel, Column as ProjectionColumn,
        Entity as ProjectionEntity,
    },
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

        let result: Result<()> = ProjectionEntity::insert_many(models)
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

    /// Count projection rows for a blockchain where desired state is Present and actual state
    /// matches `actual_state`.
    pub async fn count_desired_present_by_actual_state_for_blockchain(
        &self,
        blockchain_id: &str,
        actual_state: KcProjectionActualState,
    ) -> Result<u64> {
        self.count_desired_present_filtered(blockchain_id, actual_state)
            .await
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

        let result: Result<Vec<(String, u64)>> = ProjectionEntity::find()
            .filter(ProjectionColumn::BlockchainId.eq(blockchain_id))
            .filter(ProjectionColumn::DesiredState.eq(KcProjectionDesiredState::Present.as_u8()))
            .filter(ProjectionColumn::ActualState.eq(KcProjectionActualState::Unknown.as_u8()))
            .order_by_asc(ProjectionColumn::UpdatedAt)
            .order_by_asc(ProjectionColumn::ContractAddress)
            .order_by_asc(ProjectionColumn::KcId)
            .limit(limit as u64)
            .select_only()
            .column(ProjectionColumn::ContractAddress)
            .column(ProjectionColumn::KcId)
            .into_tuple::<(String, u64)>()
            .all(self.conn.as_ref())
            .await
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
        let p = Alias::new("p");
        let q = Alias::new("q");
        let blockchain = Alias::new("blockchain_id");
        let contract = Alias::new("contract_address");
        let kc_id = Alias::new("kc_id");
        let desired_state = Alias::new("desired_state");
        let actual_state = Alias::new("actual_state");
        let updated_at = Alias::new("updated_at");

        let mut query = Query::select();
        query
            .column((p.clone(), contract.clone()))
            .column((p.clone(), kc_id.clone()))
            .from_as(Alias::new("kc_projection_state"), p.clone())
            .join_as(
                JoinType::LeftJoin,
                Alias::new("kc_sync_queue"),
                q.clone(),
                Expr::col((q.clone(), blockchain.clone()))
                    .equals((p.clone(), blockchain.clone()))
                    .and(
                        Expr::col((q.clone(), contract.clone()))
                            .equals((p.clone(), contract.clone())),
                    )
                    .and(Expr::col((q.clone(), kc_id.clone())).equals((p.clone(), kc_id.clone()))),
            )
            .and_where(Expr::col((p.clone(), blockchain)).eq(blockchain_id))
            .and_where(
                Expr::col((p.clone(), desired_state)).eq(KcProjectionDesiredState::Present.as_u8()),
            )
            .and_where(
                Expr::col((p.clone(), actual_state)).eq(KcProjectionActualState::Pending.as_u8()),
            )
            .and_where(Expr::col((q.clone(), kc_id.clone())).is_null())
            .order_by((p.clone(), updated_at), Order::Asc)
            .order_by((p.clone(), contract), Order::Asc)
            .order_by((p, kc_id), Order::Asc)
            .limit(limit as u64);
        let statement = db.get_database_backend().build(&query);

        let result = async {
            let rows = db.query_all(statement).await?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let contract_address: String = row.try_get("", "contract_address")?;
                let kc_id: u64 = row.try_get("", "kc_id")?;
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

    async fn count_desired_present_filtered(
        &self,
        blockchain_id: &str,
        actual_state: KcProjectionActualState,
    ) -> Result<u64> {
        let started = Instant::now();
        let query_name = "count_desired_present_by_actual_state_for_blockchain";

        let query = ProjectionEntity::find()
            .filter(ProjectionColumn::BlockchainId.eq(blockchain_id))
            .filter(ProjectionColumn::DesiredState.eq(KcProjectionDesiredState::Present.as_u8()))
            .filter(ProjectionColumn::ActualState.eq(actual_state.as_u8()));

        let result = query.count(self.conn.as_ref()).await.map_err(Into::into);

        match &result {
            Ok(count) => {
                record_repository_query(
                    "kc_projection",
                    query_name,
                    "ok",
                    started.elapsed(),
                    Some(*count as usize),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_projection",
                    query_name,
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }
}
