use std::sync::Arc;

use chrono::Utc;
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect,
};

use crate::{
    error::Result,
    models::{
        kc_sync_progress::{
            ActiveModel as ProgressActiveModel, Column as ProgressColumn, Entity as ProgressEntity,
        },
        kc_sync_queue::{
            ActiveModel as QueueActiveModel, Column as QueueColumn, Entity as QueueEntity,
        },
    },
    types::{KcSyncProgressEntry, KcSyncQueueEntry},
};

#[derive(Clone)]
pub struct KcSyncRepository {
    conn: Arc<DatabaseConnection>,
}

impl KcSyncRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    // ==================== Progress Table Methods ====================

    /// Get the sync progress for a specific contract.
    /// Returns None if no progress record exists (first sync).
    pub async fn get_progress(
        &self,
        blockchain_id: &str,
        contract_address: &str,
    ) -> Result<Option<KcSyncProgressEntry>> {
        Ok(ProgressEntity::find()
            .filter(ProgressColumn::BlockchainId.eq(blockchain_id))
            .filter(ProgressColumn::ContractAddress.eq(contract_address))
            .one(self.conn.as_ref())
            .await?
            .map(Self::to_progress_entry))
    }

    /// Count tracked contracts in the sync progress table for a blockchain.
    pub async fn count_progress_contracts_for_blockchain(
        &self,
        blockchain_id: &str,
    ) -> Result<u64> {
        Ok(ProgressEntity::find()
            .filter(ProgressColumn::BlockchainId.eq(blockchain_id))
            .count(self.conn.as_ref())
            .await?)
    }

    /// Get the latest progress update timestamp for a blockchain.
    pub async fn latest_progress_updated_at_for_blockchain(
        &self,
        blockchain_id: &str,
    ) -> Result<Option<i64>> {
        Ok(ProgressEntity::find()
            .filter(ProgressColumn::BlockchainId.eq(blockchain_id))
            .order_by_desc(ProgressColumn::UpdatedAt)
            .one(self.conn.as_ref())
            .await?
            .map(|row| row.updated_at))
    }

    /// Update or insert the sync progress for a contract.
    pub async fn upsert_progress(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        last_checked_id: u64,
    ) -> Result<()> {
        let now = Utc::now().timestamp();

        let model = ProgressActiveModel {
            blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
            contract_address: ActiveValue::Set(contract_address.to_string()),
            last_checked_id: ActiveValue::Set(last_checked_id),
            updated_at: ActiveValue::Set(now),
        };

        ProgressEntity::insert(model)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([
                    ProgressColumn::BlockchainId,
                    ProgressColumn::ContractAddress,
                ])
                .update_columns([ProgressColumn::LastCheckedId, ProgressColumn::UpdatedAt])
                .to_owned(),
            )
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    // ==================== Queue Table Methods ====================

    /// Add KC IDs to the sync queue.
    /// Skips IDs that already exist in the queue.
    pub async fn enqueue_kcs(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<()> {
        if kc_ids.is_empty() {
            return Ok(());
        }

        let now = Utc::now().timestamp();

        let models: Vec<QueueActiveModel> = kc_ids
            .iter()
            .map(|&kc_id| QueueActiveModel {
                blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
                contract_address: ActiveValue::Set(contract_address.to_string()),
                kc_id: ActiveValue::Set(kc_id),
                retry_count: ActiveValue::Set(0),
                next_retry_at: ActiveValue::Set(0),
                created_at: ActiveValue::Set(now),
                last_retry_at: ActiveValue::Set(None),
            })
            .collect();

        // Insert with ON DUPLICATE KEY UPDATE to skip existing entries (MySQL compatible)
        // We update kc_id to itself, which is effectively a no-op but valid MySQL syntax
        QueueEntity::insert_many(models)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([
                    QueueColumn::BlockchainId,
                    QueueColumn::ContractAddress,
                    QueueColumn::KcId,
                ])
                .update_column(QueueColumn::KcId)
                .to_owned(),
            )
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    /// Get KCs that need syncing for a specific contract (retry_count < max_retries).
    /// Limited to `limit` results to avoid overwhelming the system.
    pub async fn get_pending_kcs_for_contract(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        now_ts: i64,
        max_retries: u32,
        limit: u64,
    ) -> Result<Vec<KcSyncQueueEntry>> {
        let rows = QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::ContractAddress.eq(contract_address))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .filter(QueueColumn::NextRetryAt.lte(now_ts))
            .order_by_asc(QueueColumn::KcId)
            .limit(limit)
            .all(self.conn.as_ref())
            .await?;

        Ok(rows.into_iter().map(Self::to_queue_entry).collect())
    }

    /// Count all queued KCs for a blockchain (regardless of retry state).
    pub async fn count_queue_total_for_blockchain(&self, blockchain_id: &str) -> Result<u64> {
        Ok(QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .count(self.conn.as_ref())
            .await?)
    }

    /// Count KCs currently due for processing (retry_count < max_retries and next_retry_at <= now).
    pub async fn count_queue_due_for_blockchain(
        &self,
        blockchain_id: &str,
        now_ts: i64,
        max_retries: u32,
    ) -> Result<u64> {
        Ok(QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .filter(QueueColumn::NextRetryAt.lte(now_ts))
            .count(self.conn.as_ref())
            .await?)
    }

    /// Count KCs waiting in retry backoff (retry_count > 0 and next_retry_at > now).
    pub async fn count_queue_retrying_for_blockchain(
        &self,
        blockchain_id: &str,
        now_ts: i64,
        max_retries: u32,
    ) -> Result<u64> {
        Ok(QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::RetryCount.gt(0))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .filter(QueueColumn::NextRetryAt.gt(now_ts))
            .count(self.conn.as_ref())
            .await?)
    }

    /// Get created_at of the oldest KC currently due for processing.
    pub async fn oldest_due_created_at_for_blockchain(
        &self,
        blockchain_id: &str,
        now_ts: i64,
        max_retries: u32,
    ) -> Result<Option<i64>> {
        Ok(QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .filter(QueueColumn::NextRetryAt.lte(now_ts))
            .order_by_asc(QueueColumn::CreatedAt)
            .one(self.conn.as_ref())
            .await?
            .map(|row| row.created_at))
    }

    /// Remove successfully synced KCs from the queue.
    pub async fn remove_kcs(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<()> {
        if kc_ids.is_empty() {
            return Ok(());
        }

        QueueEntity::delete_many()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::ContractAddress.eq(contract_address))
            .filter(QueueColumn::KcId.is_in(kc_ids.to_vec()))
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    /// Increment retry count for KCs that failed to sync.
    pub async fn increment_retry_count(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
        retry_base_delay_secs: u64,
        retry_max_delay_secs: u64,
        retry_jitter_secs: u64,
    ) -> Result<()> {
        if kc_ids.is_empty() {
            return Ok(());
        }

        let now = Utc::now().timestamp();
        let retry_base_delay_secs = retry_base_delay_secs.max(1);
        let retry_max_delay_secs = retry_max_delay_secs.max(retry_base_delay_secs);

        // Update each KC's retry count
        // SeaORM doesn't support UPDATE with increment in bulk easily,
        // so we fetch and update individually
        for &kc_id in kc_ids {
            if let Some(model) = QueueEntity::find()
                .filter(QueueColumn::BlockchainId.eq(blockchain_id))
                .filter(QueueColumn::ContractAddress.eq(contract_address))
                .filter(QueueColumn::KcId.eq(kc_id))
                .one(self.conn.as_ref())
                .await?
            {
                let next_retry_at = now.saturating_add(Self::compute_retry_delay_secs(
                    model.retry_count,
                    retry_base_delay_secs,
                    retry_max_delay_secs,
                    retry_jitter_secs,
                    kc_id,
                    now,
                ) as i64);
                let update = QueueActiveModel {
                    blockchain_id: ActiveValue::Unchanged(model.blockchain_id),
                    contract_address: ActiveValue::Unchanged(model.contract_address),
                    kc_id: ActiveValue::Unchanged(model.kc_id),
                    retry_count: ActiveValue::Set(model.retry_count.saturating_add(1)),
                    next_retry_at: ActiveValue::Set(next_retry_at),
                    created_at: ActiveValue::Unchanged(model.created_at),
                    last_retry_at: ActiveValue::Set(Some(now)),
                };
                QueueEntity::update(update).exec(self.conn.as_ref()).await?;
            }
        }

        Ok(())
    }

    fn compute_retry_delay_secs(
        current_retry_count: u32,
        retry_base_delay_secs: u64,
        retry_max_delay_secs: u64,
        retry_jitter_secs: u64,
        kc_id: u64,
        now_ts: i64,
    ) -> u64 {
        let shift = current_retry_count.min(31);
        let multiplier = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
        let backoff = retry_base_delay_secs
            .saturating_mul(multiplier)
            .min(retry_max_delay_secs);

        let available_jitter = retry_max_delay_secs.saturating_sub(backoff);
        let jitter_limit = retry_jitter_secs.min(available_jitter);
        if jitter_limit == 0 {
            return backoff;
        }

        let seed = kc_id
            .wrapping_mul(1_103_515_245)
            .wrapping_add((now_ts as u64).wrapping_mul(12_345));
        let jitter = seed % (jitter_limit.saturating_add(1));
        backoff.saturating_add(jitter)
    }

    fn to_progress_entry(model: crate::models::kc_sync_progress::Model) -> KcSyncProgressEntry {
        KcSyncProgressEntry {
            blockchain_id: model.blockchain_id,
            contract_address: model.contract_address,
            last_checked_id: model.last_checked_id,
            updated_at: model.updated_at,
        }
    }

    fn to_queue_entry(model: crate::models::kc_sync_queue::Model) -> KcSyncQueueEntry {
        KcSyncQueueEntry {
            blockchain_id: model.blockchain_id,
            contract_address: model.contract_address,
            kc_id: model.kc_id,
            retry_count: model.retry_count,
            next_retry_at: model.next_retry_at,
            created_at: model.created_at,
            last_retry_at: model.last_retry_at,
        }
    }
}
