use std::sync::Arc;

use chrono::Utc;
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder,
    error::DbErr,
};

use crate::managers::repository::models::{
    kc_sync_progress::{
        ActiveModel as ProgressActiveModel, Column as ProgressColumn, Entity as ProgressEntity,
        Model as ProgressModel,
    },
    kc_sync_queue::{
        ActiveModel as QueueActiveModel, Column as QueueColumn, Entity as QueueEntity,
        Model as QueueModel,
    },
};

pub(crate) struct KcSyncRepository {
    conn: Arc<DatabaseConnection>,
}

impl KcSyncRepository {
    pub(crate) fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    // ==================== Progress Table Methods ====================

    /// Get the sync progress for a specific contract.
    /// Returns None if no progress record exists (first sync).
    pub(crate) async fn get_progress(
        &self,
        blockchain_id: &str,
        contract_address: &str,
    ) -> Result<Option<ProgressModel>, DbErr> {
        ProgressEntity::find()
            .filter(ProgressColumn::BlockchainId.eq(blockchain_id))
            .filter(ProgressColumn::ContractAddress.eq(contract_address))
            .one(self.conn.as_ref())
            .await
    }

    /// Update or insert the sync progress for a contract.
    pub(crate) async fn upsert_progress(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        last_checked_id: u64,
    ) -> Result<(), DbErr> {
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
    pub(crate) async fn enqueue_kcs(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<(), DbErr> {
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

    /// Get all KCs that need syncing for a specific contract (retry_count < max_retries).
    pub(crate) async fn get_pending_kcs_for_contract(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        max_retries: u32,
    ) -> Result<Vec<QueueModel>, DbErr> {
        QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::ContractAddress.eq(contract_address))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .order_by_asc(QueueColumn::KcId)
            .all(self.conn.as_ref())
            .await
    }

    /// Remove successfully synced KCs from the queue.
    pub(crate) async fn remove_kcs(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<(), DbErr> {
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
    pub(crate) async fn increment_retry_count(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<(), DbErr> {
        if kc_ids.is_empty() {
            return Ok(());
        }

        let now = Utc::now().timestamp();

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
                let update = QueueActiveModel {
                    blockchain_id: ActiveValue::Unchanged(model.blockchain_id),
                    contract_address: ActiveValue::Unchanged(model.contract_address),
                    kc_id: ActiveValue::Unchanged(model.kc_id),
                    retry_count: ActiveValue::Set(model.retry_count + 1),
                    created_at: ActiveValue::Unchanged(model.created_at),
                    last_retry_at: ActiveValue::Set(Some(now)),
                };
                QueueEntity::update(update).exec(self.conn.as_ref()).await?;
            }
        }

        Ok(())
    }
}
