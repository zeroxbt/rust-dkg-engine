use std::{sync::Arc, time::Instant};

use chrono::Utc;
use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, TransactionTrait,
};

use crate::{
    error::Result,
    models::{
        kc_chain_core_metadata::{
            ActiveModel as CoreActiveModel, Column as CoreColumn, Entity as CoreEntity,
        },
        kc_chain_state_metadata::{
            ActiveModel as StateActiveModel, Column as StateColumn, Entity as StateEntity,
        },
        kc_sync_metadata_cursor::{
            ActiveModel as CursorActiveModel, Column as CursorColumn, Entity as CursorEntity,
        },
        kc_sync_queue::{
            ActiveModel as QueueActiveModel, Column as QueueColumn, Entity as QueueEntity,
        },
    },
    observability::record_repository_query,
    types::{KcSyncQueueEntry, SyncMetadataRecordInput},
};

#[derive(Clone)]
pub struct KcSyncRepository {
    conn: Arc<DatabaseConnection>,
}

impl KcSyncRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Count tracked contracts in the metadata cursor table for a blockchain.
    pub async fn count_progress_contracts_for_blockchain(
        &self,
        blockchain_id: &str,
    ) -> Result<u64> {
        let started = Instant::now();
        let result = CursorEntity::find()
            .filter(CursorColumn::BlockchainId.eq(blockchain_id))
            .count(self.conn.as_ref())
            .await
            .map_err(Into::into);

        match &result {
            Ok(count) => {
                record_repository_query(
                    "kc_sync",
                    "count_progress_contracts_for_blockchain",
                    "ok",
                    started.elapsed(),
                    Some(*count as usize),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "count_progress_contracts_for_blockchain",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Get the latest progress update timestamp for a blockchain.
    pub async fn latest_progress_updated_at_for_blockchain(
        &self,
        blockchain_id: &str,
    ) -> Result<Option<i64>> {
        let started = Instant::now();
        let result = CursorEntity::find()
            .filter(CursorColumn::BlockchainId.eq(blockchain_id))
            .order_by_desc(CursorColumn::UpdatedAt)
            .one(self.conn.as_ref())
            .await
            .map(|row| row.map(|value| value.updated_at))
            .map_err(Into::into);

        match &result {
            Ok(Some(_)) => {
                record_repository_query(
                    "kc_sync",
                    "latest_progress_updated_at_for_blockchain",
                    "ok",
                    started.elapsed(),
                    Some(1),
                );
            }
            Ok(None) => {
                record_repository_query(
                    "kc_sync",
                    "latest_progress_updated_at_for_blockchain",
                    "ok",
                    started.elapsed(),
                    Some(0),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "latest_progress_updated_at_for_blockchain",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Get metadata event backfill cursor for a contract.
    pub async fn get_metadata_progress(
        &self,
        blockchain_id: &str,
        contract_address: &str,
    ) -> Result<u64> {
        let started = Instant::now();
        let result =
            CursorEntity::find_by_id((blockchain_id.to_string(), contract_address.to_string()))
                .one(self.conn.as_ref())
                .await
                .map(|row| {
                    row.map(|value| value.last_checked_block.max(0) as u64)
                        .unwrap_or(0)
                })
                .map_err(Into::into);

        match &result {
            Ok(progress) => {
                record_repository_query(
                    "kc_sync",
                    "get_metadata_progress",
                    "ok",
                    started.elapsed(),
                    Some(*progress as usize),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "get_metadata_progress",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Upsert metadata event backfill cursor for a contract.
    pub async fn upsert_metadata_progress(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        metadata_last_checked_block: u64,
    ) -> Result<()> {
        let started = Instant::now();
        let now = Utc::now().timestamp();
        let metadata_last_checked_block = metadata_last_checked_block.min(i64::MAX as u64) as i64;
        let model = CursorActiveModel {
            blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
            contract_address: ActiveValue::Set(contract_address.to_string()),
            last_checked_block: ActiveValue::Set(metadata_last_checked_block),
            updated_at: ActiveValue::Set(now),
        };

        let result = CursorEntity::insert(model)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([
                    CursorColumn::BlockchainId,
                    CursorColumn::ContractAddress,
                ])
                .update_columns([CursorColumn::LastCheckedBlock, CursorColumn::UpdatedAt])
                .to_owned(),
            )
            .exec(self.conn.as_ref())
            .await
            .map(|_| ())
            .map_err(Into::into);

        match &result {
            Ok(()) => {
                record_repository_query(
                    "kc_sync",
                    "upsert_metadata_progress",
                    "ok",
                    started.elapsed(),
                    Some(1),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "upsert_metadata_progress",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Persist hydrated metadata rows and enqueue discovered KC IDs in a single transaction.
    ///
    /// Transaction steps:
    /// 1. Upsert KC core metadata.
    /// 2. Upsert KC state metadata (if present).
    /// 3. Enqueue discovered KC IDs (deduplicated by PK).
    /// 4. Upsert metadata cursor.
    #[allow(clippy::too_many_arguments)]
    pub async fn persist_metadata_chunk_and_enqueue(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        metadata_last_checked_block: u64,
        records: &[SyncMetadataRecordInput],
        discovered_kc_ids: &[u64],
    ) -> Result<()> {
        let started = Instant::now();
        let now = Utc::now().timestamp();
        let metadata_last_checked_block =
            Self::u64_to_i64(metadata_last_checked_block, "metadata_last_checked_block")?;

        let transaction_result = {
            let txn = self
                .conn
                .begin()
                .await
                .map_err(crate::error::RepositoryError::from)?;
            let result: Result<()> = async {
                for record in records {
                    let block_number = Self::u64_to_i64(record.block_number, "block_number")?;
                    let block_timestamp =
                        Self::u64_to_i64(record.block_timestamp, "block_timestamp")?;
                    let core_model = CoreActiveModel {
                        blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
                        contract_address: ActiveValue::Set(contract_address.to_string()),
                        kc_id: ActiveValue::Set(record.kc_id),
                        publisher_address: ActiveValue::Set(record.publisher_address.clone()),
                        block_number: ActiveValue::Set(block_number),
                        transaction_hash: ActiveValue::Set(record.transaction_hash.clone()),
                        block_timestamp: ActiveValue::Set(block_timestamp),
                        publish_operation_id: ActiveValue::Set(record.publish_operation_id.clone()),
                        source: ActiveValue::Set(Some(record.source.clone())),
                        created_at: ActiveValue::Set(now),
                        updated_at: ActiveValue::Set(now),
                    };

                    CoreEntity::insert(core_model)
                        .on_conflict(
                            sea_orm::sea_query::OnConflict::columns([
                                CoreColumn::BlockchainId,
                                CoreColumn::ContractAddress,
                                CoreColumn::KcId,
                            ])
                            .update_columns([
                                CoreColumn::PublisherAddress,
                                CoreColumn::BlockNumber,
                                CoreColumn::TransactionHash,
                                CoreColumn::BlockTimestamp,
                                CoreColumn::PublishOperationId,
                                CoreColumn::Source,
                                CoreColumn::UpdatedAt,
                            ])
                            .to_owned(),
                        )
                        .exec(&txn)
                        .await?;

                    if let Some(state) = &record.state {
                        let range_start_token_id =
                            Self::u64_to_u32(state.range_start_token_id, "range_start_token_id")?;
                        let range_end_token_id =
                            Self::u64_to_u32(state.range_end_token_id, "range_end_token_id")?;
                        let end_epoch = Self::u64_to_i64(state.end_epoch, "end_epoch")?;
                        let state_observed_block =
                            Self::u64_to_i64(record.block_number, "state_observed_block")?;

                        let state_model = StateActiveModel {
                            blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
                            contract_address: ActiveValue::Set(contract_address.to_string()),
                            kc_id: ActiveValue::Set(record.kc_id),
                            range_start_token_id: ActiveValue::Set(range_start_token_id),
                            range_end_token_id: ActiveValue::Set(range_end_token_id),
                            burned_mode: ActiveValue::Set(state.burned_mode),
                            burned_payload: ActiveValue::Set(state.burned_payload.clone()),
                            end_epoch: ActiveValue::Set(end_epoch),
                            latest_merkle_root: ActiveValue::Set(state.latest_merkle_root.clone()),
                            state_observed_block: ActiveValue::Set(state_observed_block),
                            state_updated_at: ActiveValue::Set(now),
                            private_graph_mode: ActiveValue::Set(None),
                            private_graph_payload: ActiveValue::Set(None),
                            created_at: ActiveValue::Set(now),
                            updated_at: ActiveValue::Set(now),
                        };

                        StateEntity::insert(state_model)
                            .on_conflict(
                                sea_orm::sea_query::OnConflict::columns([
                                    StateColumn::BlockchainId,
                                    StateColumn::ContractAddress,
                                    StateColumn::KcId,
                                ])
                                .update_columns([
                                    StateColumn::RangeStartTokenId,
                                    StateColumn::RangeEndTokenId,
                                    StateColumn::BurnedMode,
                                    StateColumn::BurnedPayload,
                                    StateColumn::EndEpoch,
                                    StateColumn::LatestMerkleRoot,
                                    StateColumn::StateObservedBlock,
                                    StateColumn::StateUpdatedAt,
                                    StateColumn::PrivateGraphMode,
                                    StateColumn::PrivateGraphPayload,
                                    StateColumn::UpdatedAt,
                                ])
                                .to_owned(),
                            )
                            .exec(&txn)
                            .await?;
                    }
                }

                if !discovered_kc_ids.is_empty() {
                    let queue_models: Vec<QueueActiveModel> = discovered_kc_ids
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

                    QueueEntity::insert_many(queue_models)
                        .on_conflict(
                            sea_orm::sea_query::OnConflict::columns([
                                QueueColumn::BlockchainId,
                                QueueColumn::ContractAddress,
                                QueueColumn::KcId,
                            ])
                            .do_nothing()
                            .to_owned(),
                        )
                        .exec(&txn)
                        .await?;
                }

                let cursor_model = CursorActiveModel {
                    blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
                    contract_address: ActiveValue::Set(contract_address.to_string()),
                    last_checked_block: ActiveValue::Set(metadata_last_checked_block),
                    updated_at: ActiveValue::Set(now),
                };
                CursorEntity::insert(cursor_model)
                    .on_conflict(
                        sea_orm::sea_query::OnConflict::columns([
                            CursorColumn::BlockchainId,
                            CursorColumn::ContractAddress,
                        ])
                        .update_columns([CursorColumn::LastCheckedBlock, CursorColumn::UpdatedAt])
                        .to_owned(),
                    )
                    .exec(&txn)
                    .await?;

                Ok(())
            }
            .await;

            match result {
                Ok(()) => {
                    txn.commit()
                        .await
                        .map_err(crate::error::RepositoryError::from)?;
                    Ok(())
                }
                Err(error) => {
                    let _ = txn.rollback().await;
                    Err(error)
                }
            }
        };

        match &transaction_result {
            Ok(()) => {
                record_repository_query(
                    "kc_sync",
                    "persist_metadata_chunk_and_enqueue",
                    "ok",
                    started.elapsed(),
                    Some(records.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "persist_metadata_chunk_and_enqueue",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        transaction_result
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
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query("kc_sync", "enqueue_kcs", "ok", started.elapsed(), Some(0));
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

        // Insert with ON CONFLICT DO NOTHING to skip existing entries.
        let result = QueueEntity::insert_many(models)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([
                    QueueColumn::BlockchainId,
                    QueueColumn::ContractAddress,
                    QueueColumn::KcId,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec(self.conn.as_ref())
            .await
            .map(|_| ())
            .map_err(Into::into);

        match &result {
            Ok(()) => {
                record_repository_query(
                    "kc_sync",
                    "enqueue_kcs",
                    "ok",
                    started.elapsed(),
                    Some(kc_ids.len()),
                );
            }
            Err(_) => {
                record_repository_query("kc_sync", "enqueue_kcs", "error", started.elapsed(), None);
            }
        }

        result
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
        let started = Instant::now();
        let result = QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::ContractAddress.eq(contract_address))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .filter(QueueColumn::NextRetryAt.lte(now_ts))
            .order_by_asc(QueueColumn::KcId)
            .limit(limit)
            .all(self.conn.as_ref())
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(Self::to_queue_entry)
                    .collect::<Vec<_>>()
            })
            .map_err(Into::into);

        match &result {
            Ok(rows) => {
                record_repository_query(
                    "kc_sync",
                    "get_pending_kcs_for_contract",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "get_pending_kcs_for_contract",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Get only due KC IDs for a specific contract.
    pub async fn get_due_kc_ids_for_contract(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        now_ts: i64,
        max_retries: u32,
        limit: u64,
    ) -> Result<Vec<u64>> {
        let started = Instant::now();
        let result = QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::ContractAddress.eq(contract_address))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .filter(QueueColumn::NextRetryAt.lte(now_ts))
            .order_by_asc(QueueColumn::KcId)
            .limit(limit)
            .select_only()
            .column(QueueColumn::KcId)
            .into_tuple()
            .all(self.conn.as_ref())
            .await
            .map_err(Into::into);

        match &result {
            Ok(rows) => {
                record_repository_query(
                    "kc_sync",
                    "get_due_kc_ids_for_contract",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "get_due_kc_ids_for_contract",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Get due queue rows for a blockchain in FIFO order by `created_at`.
    pub async fn get_due_queue_entries_fifo_for_blockchain(
        &self,
        blockchain_id: &str,
        now_ts: i64,
        max_retries: u32,
        limit: u64,
    ) -> Result<Vec<KcSyncQueueEntry>> {
        let started = Instant::now();
        let result = QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .filter(QueueColumn::NextRetryAt.lte(now_ts))
            .order_by_asc(QueueColumn::CreatedAt)
            .order_by_asc(QueueColumn::ContractAddress)
            .order_by_asc(QueueColumn::KcId)
            .limit(limit)
            .all(self.conn.as_ref())
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(Self::to_queue_entry)
                    .collect::<Vec<_>>()
            })
            .map_err(Into::into);

        match &result {
            Ok(rows) => {
                record_repository_query(
                    "kc_sync",
                    "get_due_queue_entries_fifo_for_blockchain",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "get_due_queue_entries_fifo_for_blockchain",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Count all queued KCs for a blockchain (regardless of retry state).
    pub async fn count_queue_total_for_blockchain(&self, blockchain_id: &str) -> Result<u64> {
        let started = Instant::now();
        let result = QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .count(self.conn.as_ref())
            .await
            .map_err(Into::into);

        match &result {
            Ok(count) => {
                record_repository_query(
                    "kc_sync",
                    "count_queue_total_for_blockchain",
                    "ok",
                    started.elapsed(),
                    Some(*count as usize),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "count_queue_total_for_blockchain",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Count KCs currently due for processing (retry_count < max_retries and next_retry_at <= now).
    pub async fn count_queue_due_for_blockchain(
        &self,
        blockchain_id: &str,
        now_ts: i64,
        max_retries: u32,
    ) -> Result<u64> {
        let started = Instant::now();
        let result = QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .filter(QueueColumn::NextRetryAt.lte(now_ts))
            .count(self.conn.as_ref())
            .await
            .map_err(Into::into);

        match &result {
            Ok(count) => {
                record_repository_query(
                    "kc_sync",
                    "count_queue_due_for_blockchain",
                    "ok",
                    started.elapsed(),
                    Some(*count as usize),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "count_queue_due_for_blockchain",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Count KCs waiting in retry backoff (retry_count > 0 and next_retry_at > now).
    pub async fn count_queue_retrying_for_blockchain(
        &self,
        blockchain_id: &str,
        now_ts: i64,
        max_retries: u32,
    ) -> Result<u64> {
        let started = Instant::now();
        let result = QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::RetryCount.gt(0))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .filter(QueueColumn::NextRetryAt.gt(now_ts))
            .count(self.conn.as_ref())
            .await
            .map_err(Into::into);

        match &result {
            Ok(count) => {
                record_repository_query(
                    "kc_sync",
                    "count_queue_retrying_for_blockchain",
                    "ok",
                    started.elapsed(),
                    Some(*count as usize),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "count_queue_retrying_for_blockchain",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Get created_at of the oldest KC currently due for processing.
    pub async fn oldest_due_created_at_for_blockchain(
        &self,
        blockchain_id: &str,
        now_ts: i64,
        max_retries: u32,
    ) -> Result<Option<i64>> {
        let started = Instant::now();
        let result = QueueEntity::find()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::RetryCount.lt(max_retries))
            .filter(QueueColumn::NextRetryAt.lte(now_ts))
            .order_by_asc(QueueColumn::CreatedAt)
            .one(self.conn.as_ref())
            .await
            .map(|row| row.map(|value| value.created_at))
            .map_err(Into::into);

        match &result {
            Ok(Some(_)) => {
                record_repository_query(
                    "kc_sync",
                    "oldest_due_created_at_for_blockchain",
                    "ok",
                    started.elapsed(),
                    Some(1),
                );
            }
            Ok(None) => {
                record_repository_query(
                    "kc_sync",
                    "oldest_due_created_at_for_blockchain",
                    "ok",
                    started.elapsed(),
                    Some(0),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "oldest_due_created_at_for_blockchain",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Remove successfully synced KCs from the queue.
    pub async fn remove_kcs(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<()> {
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query("kc_sync", "remove_kcs", "ok", started.elapsed(), Some(0));
            return Ok(());
        }

        let result = QueueEntity::delete_many()
            .filter(QueueColumn::BlockchainId.eq(blockchain_id))
            .filter(QueueColumn::ContractAddress.eq(contract_address))
            .filter(QueueColumn::KcId.is_in(kc_ids.to_vec()))
            .exec(self.conn.as_ref())
            .await
            .map(|_| ())
            .map_err(Into::into);

        match &result {
            Ok(()) => {
                record_repository_query(
                    "kc_sync",
                    "remove_kcs",
                    "ok",
                    started.elapsed(),
                    Some(kc_ids.len()),
                );
            }
            Err(_) => {
                record_repository_query("kc_sync", "remove_kcs", "error", started.elapsed(), None);
            }
        }

        result
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
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query(
                "kc_sync",
                "increment_retry_count",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(());
        }

        let now = Utc::now().timestamp();
        let retry_base_delay_secs = retry_base_delay_secs.max(1);
        let retry_max_delay_secs = retry_max_delay_secs.max(retry_base_delay_secs);

        // Update each KC's retry count
        // SeaORM doesn't support UPDATE with increment in bulk easily,
        // so we fetch and update individually
        let result = async {
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
        .await;

        match &result {
            Ok(()) => {
                record_repository_query(
                    "kc_sync",
                    "increment_retry_count",
                    "ok",
                    started.elapsed(),
                    Some(kc_ids.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_sync",
                    "increment_retry_count",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
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

    fn u64_to_i64(value: u64, field: &'static str) -> Result<i64> {
        i64::try_from(value).map_err(|_| {
            crate::error::RepositoryError::SyncMetadata(format!(
                "{field} value {value} exceeds i64 range"
            ))
        })
    }

    fn u64_to_u32(value: u64, field: &'static str) -> Result<u32> {
        u32::try_from(value).map_err(|_| {
            crate::error::RepositoryError::SyncMetadata(format!(
                "{field} value {value} exceeds u32 range"
            ))
        })
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
