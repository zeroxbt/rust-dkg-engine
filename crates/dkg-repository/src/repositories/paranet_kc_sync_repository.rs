use std::sync::Arc;

use chrono::Utc;
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect,
};

use crate::{
    error::Result,
    models::paranet_kc_sync::{ActiveModel, Column, Entity, Model},
    types::ParanetKcSyncEntry,
};

pub const STATUS_PENDING: &str = "pending";
pub const STATUS_SYNCED: &str = "synced";

#[derive(Clone)]
pub struct ParanetKcSyncRepository {
    conn: Arc<DatabaseConnection>,
}

impl ParanetKcSyncRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    pub async fn count_discovered(&self, paranet_ual: &str) -> Result<u64> {
        Entity::find()
            .filter(Column::ParanetUal.eq(paranet_ual))
            .count(self.conn.as_ref())
            .await
            .map_err(Into::into)
    }

    pub async fn enqueue_locators(
        &self,
        paranet_ual: &str,
        blockchain_id: &str,
        paranet_id: &str,
        kc_uals: &[String],
    ) -> Result<()> {
        if kc_uals.is_empty() {
            return Ok(());
        }

        let now = Utc::now().timestamp();
        let models: Vec<ActiveModel> = kc_uals
            .iter()
            .map(|kc_ual| ActiveModel {
                paranet_ual: ActiveValue::Set(paranet_ual.to_string()),
                kc_ual: ActiveValue::Set(kc_ual.clone()),
                blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
                paranet_id: ActiveValue::Set(paranet_id.to_string()),
                retry_count: ActiveValue::Set(0),
                next_retry_at: ActiveValue::Set(0),
                last_error: ActiveValue::Set(None),
                status: ActiveValue::Set(STATUS_PENDING.to_string()),
                created_at: ActiveValue::Set(now),
                updated_at: ActiveValue::Set(now),
            })
            .collect();

        Entity::insert_many(models)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([Column::ParanetUal, Column::KcUal])
                    // MySQL-compatible no-op upsert to ignore duplicates.
                    // `DO NOTHING` can generate invalid SQL on some MySQL setups.
                    .update_column(Column::KcUal)
                    .to_owned(),
            )
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    pub async fn get_due_pending_batch(
        &self,
        blockchain_id: &str,
        paranet_ual: &str,
        now_ts: i64,
        retries_limit: u32,
        limit: u64,
    ) -> Result<Vec<ParanetKcSyncEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let rows = Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .filter(Column::ParanetUal.eq(paranet_ual))
            .filter(Column::Status.eq(STATUS_PENDING))
            .filter(Column::RetryCount.lt(retries_limit))
            .filter(Column::NextRetryAt.lte(now_ts))
            .order_by_asc(Column::KcUal)
            .limit(limit)
            .all(self.conn.as_ref())
            .await?;

        Ok(rows
            .into_iter()
            .map(|row: Model| ParanetKcSyncEntry {
                paranet_ual: row.paranet_ual,
                kc_ual: row.kc_ual,
                retry_count: row.retry_count,
            })
            .collect())
    }

    pub async fn mark_synced(&self, paranet_ual: &str, kc_ual: &str) -> Result<()> {
        let now = Utc::now().timestamp();
        let Some(existing) = Entity::find()
            .filter(Column::ParanetUal.eq(paranet_ual))
            .filter(Column::KcUal.eq(kc_ual))
            .one(self.conn.as_ref())
            .await?
        else {
            return Ok(());
        };

        let update = ActiveModel {
            paranet_ual: ActiveValue::Unchanged(existing.paranet_ual),
            kc_ual: ActiveValue::Unchanged(existing.kc_ual),
            blockchain_id: ActiveValue::Unchanged(existing.blockchain_id),
            paranet_id: ActiveValue::Unchanged(existing.paranet_id),
            retry_count: ActiveValue::Unchanged(existing.retry_count),
            next_retry_at: ActiveValue::Set(0),
            last_error: ActiveValue::Set(None),
            status: ActiveValue::Set(STATUS_SYNCED.to_string()),
            created_at: ActiveValue::Unchanged(existing.created_at),
            updated_at: ActiveValue::Set(now),
        };

        Entity::update(update).exec(self.conn.as_ref()).await?;
        Ok(())
    }

    pub async fn mark_failed_attempt(
        &self,
        paranet_ual: &str,
        kc_ual: &str,
        now_ts: i64,
        retry_delay_secs: u64,
        error: &str,
    ) -> Result<()> {
        let Some(existing) = Entity::find()
            .filter(Column::ParanetUal.eq(paranet_ual))
            .filter(Column::KcUal.eq(kc_ual))
            .one(self.conn.as_ref())
            .await?
        else {
            return Ok(());
        };

        let next_retry_at = now_ts.saturating_add(retry_delay_secs as i64);
        let update = ActiveModel {
            paranet_ual: ActiveValue::Unchanged(existing.paranet_ual),
            kc_ual: ActiveValue::Unchanged(existing.kc_ual),
            blockchain_id: ActiveValue::Unchanged(existing.blockchain_id),
            paranet_id: ActiveValue::Unchanged(existing.paranet_id),
            retry_count: ActiveValue::Set(existing.retry_count.saturating_add(1)),
            next_retry_at: ActiveValue::Set(next_retry_at),
            last_error: ActiveValue::Set(Some(error.to_string())),
            status: ActiveValue::Set(STATUS_PENDING.to_string()),
            created_at: ActiveValue::Unchanged(existing.created_at),
            updated_at: ActiveValue::Set(now_ts),
        };

        Entity::update(update).exec(self.conn.as_ref()).await?;
        Ok(())
    }
}
