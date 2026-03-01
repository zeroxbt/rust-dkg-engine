use std::{sync::Arc, time::Instant};

use chrono::Utc;
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, sea_query::Expr,
};

use crate::{
    error::Result,
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
                attempt_count: ActiveValue::Set(0),
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
                ProjectionColumn::AttemptCount,
                Expr::col(ProjectionColumn::AttemptCount).add(1u32),
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
}
