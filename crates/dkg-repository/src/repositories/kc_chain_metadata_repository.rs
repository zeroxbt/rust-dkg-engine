use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use chrono::Utc;
use sea_orm::sea_query::{Expr, Value};
use sea_orm::{
    ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, Statement,
};

use crate::{
    error::{RepositoryError, Result},
    models::{
        kc_chain_core_metadata::{
            ActiveModel as CoreActiveModel, Column as CoreColumn, Entity as CoreEntity,
            Model as CoreModel,
        },
        kc_chain_state_metadata::{
            ActiveModel as StateActiveModel, Column as StateColumn, Entity as StateEntity,
            Model as StateModel,
        },
    },
    observability::record_repository_query,
    types::{KcChainMetadataEntry, KcChainReadyKcStateMetadataEntry},
};

/// Result of the two NOT EXISTS gap-detection queries.
/// `ends_of_runs[i]` paired with `starts_of_runs[i+1]` gives internal gap boundaries.
/// `starts_of_runs[0].kc_id > 1` indicates a leading gap.
/// `ends_of_runs.last()` gives the tail start block.
#[derive(Debug, Clone)]
pub struct GapBoundaries {
    /// (kc_id, block_number) for the last KC of each consecutive run, sorted by kc_id.
    pub ends_of_runs: Vec<(u64, u64)>,
    /// (kc_id, block_number) for the first KC of each consecutive run (including run starting
    /// at 1 if it exists), sorted by kc_id.
    pub starts_of_runs: Vec<(u64, u64)>,
}

#[derive(Clone)]
pub struct KcChainMetadataRepository {
    conn: Arc<DatabaseConnection>,
}

impl KcChainMetadataRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Count KC rows with core metadata for a blockchain.
    pub async fn count_core_metadata_for_blockchain(&self, blockchain_id: &str) -> Result<u64> {
        let started = Instant::now();
        let result = CoreEntity::find()
            .filter(CoreColumn::BlockchainId.eq(blockchain_id))
            .filter(CoreColumn::PublisherAddress.is_not_null())
            .count(self.conn.as_ref())
            .await
            .map_err(Into::into);

        match &result {
            Ok(count) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "count_core_metadata_for_blockchain",
                    "ok",
                    started.elapsed(),
                    Some(*count as usize),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "count_core_metadata_for_blockchain",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Count KC rows with core metadata for a blockchain and specific source.
    pub async fn count_core_metadata_for_blockchain_by_source(
        &self,
        blockchain_id: &str,
        source: &str,
    ) -> Result<u64> {
        let started = Instant::now();
        let result = CoreEntity::find()
            .filter(CoreColumn::BlockchainId.eq(blockchain_id))
            .filter(CoreColumn::PublisherAddress.is_not_null())
            .filter(CoreColumn::Source.eq(source))
            .count(self.conn.as_ref())
            .await
            .map_err(Into::into);

        match &result {
            Ok(count) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "count_core_metadata_for_blockchain_by_source",
                    "ok",
                    started.elapsed(),
                    Some(*count as usize),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "count_core_metadata_for_blockchain_by_source",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Backward-compatible alias for upserting core chain metadata.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_id: u64,
        publisher_address: Option<&str>,
        block_number: u64,
        transaction_hash: &str,
        block_timestamp: u64,
        publish_operation_id: &str,
        source: Option<&str>,
    ) -> Result<()> {
        self.upsert_core_metadata(
            blockchain_id,
            contract_address,
            kc_id,
            publisher_address,
            block_number,
            transaction_hash,
            block_timestamp,
            publish_operation_id,
            source,
        )
        .await
    }

    /// Upsert canonical KC core metadata sourced from chain events or backfill.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_core_metadata(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_id: u64,
        publisher_address: Option<&str>,
        block_number: u64,
        transaction_hash: &str,
        block_timestamp: u64,
        publish_operation_id: &str,
        source: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let block_number = Self::u64_to_i64(block_number, "block_number")?;
        let block_timestamp = Self::u64_to_i64(block_timestamp, "block_timestamp")?;

        let model = CoreActiveModel {
            blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
            contract_address: ActiveValue::Set(contract_address.to_string()),
            kc_id: ActiveValue::Set(kc_id),
            publisher_address: ActiveValue::Set(publisher_address.map(ToString::to_string)),
            block_number: ActiveValue::Set(block_number),
            transaction_hash: ActiveValue::Set(transaction_hash.to_string()),
            block_timestamp: ActiveValue::Set(block_timestamp),
            publish_operation_id: ActiveValue::Set(publish_operation_id.to_string()),
            source: ActiveValue::Set(source.map(ToString::to_string)),
            created_at: ActiveValue::Set(now),
            updated_at: ActiveValue::Set(now),
        };

        CoreEntity::insert(model)
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
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    /// Upsert mutable sync state for an existing KC row (or create placeholder row if absent).
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_kc_state_metadata(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_id: u64,
        range_start_token_id: u64,
        range_end_token_id: u64,
        burned_mode: u32,
        burned_payload: &[u8],
        end_epoch: u64,
        latest_merkle_root: &str,
        state_observed_block: u64,
        _source: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let range_start_token_id = Self::u64_to_u32(range_start_token_id, "range_start_token_id")?;
        let range_end_token_id = Self::u64_to_u32(range_end_token_id, "range_end_token_id")?;
        let end_epoch = Self::u64_to_i64(end_epoch, "end_epoch")?;
        let state_observed_block = Self::u64_to_i64(state_observed_block, "state_observed_block")?;

        let model = StateActiveModel {
            blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
            contract_address: ActiveValue::Set(contract_address.to_string()),
            kc_id: ActiveValue::Set(kc_id),
            range_start_token_id: ActiveValue::Set(range_start_token_id),
            range_end_token_id: ActiveValue::Set(range_end_token_id),
            burned_mode: ActiveValue::Set(burned_mode),
            burned_payload: ActiveValue::Set(burned_payload.to_vec()),
            end_epoch: ActiveValue::Set(end_epoch),
            latest_merkle_root: ActiveValue::Set(latest_merkle_root.to_string()),
            state_observed_block: ActiveValue::Set(state_observed_block),
            state_updated_at: ActiveValue::Set(now),
            private_graph_mode: ActiveValue::Set(None),
            private_graph_payload: ActiveValue::Set(None),
            created_at: ActiveValue::Set(now),
            updated_at: ActiveValue::Set(now),
        };

        StateEntity::insert(model)
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
                    StateColumn::UpdatedAt,
                ])
                .to_owned(),
            )
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    /// Upsert private graph encoding for a KC without mutating canonical chain metadata fields.
    pub async fn upsert_private_graph_encoding(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_id: u64,
        private_graph_mode: Option<u32>,
        private_graph_payload: Option<&[u8]>,
        _source: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let update_result = StateEntity::update_many()
            .filter(StateColumn::BlockchainId.eq(blockchain_id))
            .filter(StateColumn::ContractAddress.eq(contract_address))
            .filter(StateColumn::KcId.eq(kc_id))
            .col_expr(
                StateColumn::PrivateGraphMode,
                Expr::value(private_graph_mode),
            )
            .col_expr(
                StateColumn::PrivateGraphPayload,
                Expr::value(private_graph_payload.map(ToOwned::to_owned)),
            )
            .col_expr(StateColumn::UpdatedAt, Expr::value(now))
            .exec(self.conn.as_ref())
            .await?;

        if update_result.rows_affected == 0 {
            return Err(RepositoryError::NotFound(format!(
                "missing kc_chain_state_metadata row for blockchain_id={blockchain_id}, contract_address={contract_address}, kc_id={kc_id}"
            )));
        }

        Ok(())
    }

    /// Get complete metadata for a single KC.
    pub async fn get_complete(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_id: u64,
    ) -> Result<Option<KcChainMetadataEntry>> {
        let out = self
            .get_many_complete(blockchain_id, contract_address, &[kc_id])
            .await?;
        Ok(out.into_values().next())
    }

    /// Return complete metadata rows for a KC id set.
    pub async fn get_many_complete(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<HashMap<u64, KcChainMetadataEntry>> {
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query(
                "kc_chain_metadata",
                "get_many_complete",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(HashMap::new());
        }

        let result = async {
            let core_rows = CoreEntity::find()
                .filter(CoreColumn::BlockchainId.eq(blockchain_id))
                .filter(CoreColumn::ContractAddress.eq(contract_address))
                .filter(CoreColumn::KcId.is_in(kc_ids.to_vec()))
                .filter(CoreColumn::PublisherAddress.is_not_null())
                .all(self.conn.as_ref())
                .await?;

            let state_rows = StateEntity::find()
                .filter(StateColumn::BlockchainId.eq(blockchain_id))
                .filter(StateColumn::ContractAddress.eq(contract_address))
                .filter(StateColumn::KcId.is_in(kc_ids.to_vec()))
                .all(self.conn.as_ref())
                .await?;
            let state_by_id: HashMap<u64, StateModel> =
                state_rows.into_iter().map(|row| (row.kc_id, row)).collect();

            let mut out = HashMap::with_capacity(core_rows.len());
            for core in core_rows {
                let kc_id = core.kc_id;
                if let Some(entry) = Self::to_complete_entry(core, state_by_id.get(&kc_id)) {
                    out.insert(entry.kc_id, entry);
                }
            }
            Ok(out)
        }
        .await;

        match &result {
            Ok(rows) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "get_many_complete",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "get_many_complete",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Return only KC IDs that currently have complete metadata.
    pub async fn get_kc_ids_with_complete_metadata(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<HashSet<u64>> {
        let entries = self
            .get_many_complete(blockchain_id, contract_address, kc_ids)
            .await?;
        Ok(entries.into_keys().collect())
    }

    /// Return complete metadata + state rows for a KC id set.
    pub async fn get_many_ready_with_kc_state_metadata(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<HashMap<u64, KcChainReadyKcStateMetadataEntry>> {
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query(
                "kc_chain_metadata",
                "get_many_ready_with_kc_state_metadata",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(HashMap::new());
        }

        let result = async {
            let core_rows = CoreEntity::find()
                .filter(CoreColumn::BlockchainId.eq(blockchain_id))
                .filter(CoreColumn::ContractAddress.eq(contract_address))
                .filter(CoreColumn::KcId.is_in(kc_ids.to_vec()))
                .filter(CoreColumn::PublisherAddress.is_not_null())
                .all(self.conn.as_ref())
                .await?;
            let core_by_id: HashMap<u64, CoreModel> =
                core_rows.into_iter().map(|row| (row.kc_id, row)).collect();

            let state_rows = StateEntity::find()
                .filter(StateColumn::BlockchainId.eq(blockchain_id))
                .filter(StateColumn::ContractAddress.eq(contract_address))
                .filter(StateColumn::KcId.is_in(kc_ids.to_vec()))
                .filter(StateColumn::RangeStartTokenId.is_not_null())
                .filter(StateColumn::RangeEndTokenId.is_not_null())
                .filter(StateColumn::BurnedMode.is_not_null())
                .filter(StateColumn::BurnedPayload.is_not_null())
                .filter(StateColumn::EndEpoch.is_not_null())
                .filter(StateColumn::LatestMerkleRoot.is_not_null())
                .filter(StateColumn::StateObservedBlock.is_not_null())
                .all(self.conn.as_ref())
                .await?;

            let mut out = HashMap::with_capacity(state_rows.len());
            for state in state_rows {
                if let Some(core) = core_by_id.get(&state.kc_id)
                    && let Some(entry) = Self::to_ready_kc_state_metadata_entry(core, &state)
                {
                    out.insert(entry.kc_id, entry);
                }
            }
            Ok(out)
        }
        .await;

        match &result {
            Ok(rows) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "get_many_ready_with_kc_state_metadata",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "get_many_ready_with_kc_state_metadata",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Return KC IDs from the provided set that are missing sync state.
    pub async fn get_ids_missing_kc_state_metadata(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<HashSet<u64>> {
        let started = Instant::now();
        if kc_ids.is_empty() {
            record_repository_query(
                "kc_chain_metadata",
                "get_ids_missing_kc_state_metadata",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(HashSet::new());
        }

        let result = async {
            let rows = StateEntity::find()
                .filter(StateColumn::BlockchainId.eq(blockchain_id))
                .filter(StateColumn::ContractAddress.eq(contract_address))
                .filter(StateColumn::KcId.is_in(kc_ids.to_vec()))
                .all(self.conn.as_ref())
                .await?;

            let mut missing = HashSet::new();
            let mut seen = HashSet::new();
            for row in rows {
                seen.insert(row.kc_id);
                if row.end_epoch <= 0 {
                    missing.insert(row.kc_id);
                }
            }

            for kc_id in kc_ids {
                if !seen.contains(kc_id) {
                    missing.insert(*kc_id);
                }
            }
            Ok(missing)
        }
        .await;

        match &result {
            Ok(rows) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "get_ids_missing_kc_state_metadata",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "get_ids_missing_kc_state_metadata",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Find KC ID sequence gap boundaries for a contract using two NOT EXISTS queries.
    ///
    /// Returns `GapBoundaries` containing:
    /// - `ends_of_runs`: last KC of each consecutive run (sorted by kc_id)
    /// - `starts_of_runs`: first KC of each consecutive run (sorted by kc_id)
    ///
    /// The caller uses these to compute scan ranges:
    /// - leading gap: `starts_of_runs[0].kc_id > 1`
    /// - internal gaps: `zip(ends[..n-1], starts[1..])`
    /// - tail: `ends[last]`
    pub async fn find_gap_boundaries(
        &self,
        blockchain_id: &str,
        contract_address: &str,
    ) -> Result<GapBoundaries> {
        let started = Instant::now();
        let db = self.conn.as_ref();

        let ends_sql = Statement::from_sql_and_values(
            db.get_database_backend(),
            r#"
            SELECT t.kc_id, t.block_number
            FROM kc_chain_core_metadata t
            WHERE t.blockchain_id = ?
              AND t.contract_address = ?
              AND NOT EXISTS (
                SELECT 1 FROM kc_chain_core_metadata
                WHERE blockchain_id = t.blockchain_id
                  AND contract_address = t.contract_address
                  AND kc_id = t.kc_id + 1
              )
            ORDER BY t.kc_id ASC
            "#,
            [
                Value::String(Some(Box::new(blockchain_id.to_string()))),
                Value::String(Some(Box::new(contract_address.to_string()))),
            ],
        );

        let starts_sql = Statement::from_sql_and_values(
            db.get_database_backend(),
            r#"
            SELECT t.kc_id, t.block_number
            FROM kc_chain_core_metadata t
            WHERE t.blockchain_id = ?
              AND t.contract_address = ?
              AND NOT EXISTS (
                SELECT 1 FROM kc_chain_core_metadata
                WHERE blockchain_id = t.blockchain_id
                  AND contract_address = t.contract_address
                  AND kc_id = t.kc_id - 1
              )
            ORDER BY t.kc_id ASC
            "#,
            [
                Value::String(Some(Box::new(blockchain_id.to_string()))),
                Value::String(Some(Box::new(contract_address.to_string()))),
            ],
        );

        let result = async {
            let ends_rows = db
                .query_all(ends_sql)
                .await
                .map_err(RepositoryError::Database)?;
            let starts_rows = db
                .query_all(starts_sql)
                .await
                .map_err(RepositoryError::Database)?;

            let mut ends_of_runs = Vec::with_capacity(ends_rows.len());
            for row in ends_rows {
                let kc_id: u64 = row
                    .try_get("", "kc_id")
                    .map_err(RepositoryError::Database)?;
                let block_number: i64 = row
                    .try_get("", "block_number")
                    .map_err(RepositoryError::Database)?;
                ends_of_runs.push((kc_id, block_number.max(0) as u64));
            }

            let mut starts_of_runs = Vec::with_capacity(starts_rows.len());
            for row in starts_rows {
                let kc_id: u64 = row
                    .try_get("", "kc_id")
                    .map_err(RepositoryError::Database)?;
                let block_number: i64 = row
                    .try_get("", "block_number")
                    .map_err(RepositoryError::Database)?;
                starts_of_runs.push((kc_id, block_number.max(0) as u64));
            }

            Ok(GapBoundaries {
                ends_of_runs,
                starts_of_runs,
            })
        }
        .await;

        match &result {
            Ok(boundaries) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "find_gap_boundaries",
                    "ok",
                    started.elapsed(),
                    Some(
                        boundaries
                            .ends_of_runs
                            .len()
                            .saturating_add(boundaries.starts_of_runs.len()),
                    ),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "find_gap_boundaries",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    fn to_complete_entry(
        core: CoreModel,
        state: Option<&StateModel>,
    ) -> Option<KcChainMetadataEntry> {
        Some(KcChainMetadataEntry {
            blockchain_id: core.blockchain_id,
            contract_address: core.contract_address,
            kc_id: core.kc_id,
            publisher_address: core.publisher_address?,
            block_number: u64::try_from(core.block_number).ok()?,
            transaction_hash: core.transaction_hash,
            block_timestamp: u64::try_from(core.block_timestamp).ok()?,
            range_start_token_id: state.map(|s| u64::from(s.range_start_token_id)),
            range_end_token_id: state.map(|s| u64::from(s.range_end_token_id)),
            burned_mode: state.map(|s| s.burned_mode),
            burned_payload: state.map(|s| s.burned_payload.clone()),
            end_epoch: state.and_then(|s| Self::i64_to_u64(s.end_epoch)),
            latest_merkle_root: state.map(|s| s.latest_merkle_root.clone()),
            state_observed_block: state.and_then(|s| Self::i64_to_u64(s.state_observed_block)),
            state_updated_at: state.map_or(0, |s| s.state_updated_at),
            private_graph_mode: state.and_then(|s| s.private_graph_mode),
            private_graph_payload: state.and_then(|s| s.private_graph_payload.clone()),
            publish_operation_id: Some(core.publish_operation_id),
            source: core.source,
            created_at: core.created_at,
            updated_at: core.updated_at,
        })
    }

    fn to_ready_kc_state_metadata_entry(
        core: &CoreModel,
        state: &StateModel,
    ) -> Option<KcChainReadyKcStateMetadataEntry> {
        Some(KcChainReadyKcStateMetadataEntry {
            blockchain_id: core.blockchain_id.clone(),
            contract_address: core.contract_address.clone(),
            kc_id: core.kc_id,
            publisher_address: core.publisher_address.clone()?,
            block_number: u64::try_from(core.block_number).ok()?,
            transaction_hash: core.transaction_hash.clone(),
            block_timestamp: u64::try_from(core.block_timestamp).ok()?,
            range_start_token_id: u64::from(state.range_start_token_id),
            range_end_token_id: u64::from(state.range_end_token_id),
            burned_mode: state.burned_mode,
            burned_payload: state.burned_payload.clone(),
            end_epoch: u64::try_from(state.end_epoch).ok()?,
            latest_merkle_root: state.latest_merkle_root.clone(),
            state_observed_block: u64::try_from(state.state_observed_block).ok()?,
        })
    }

    fn u64_to_i64(value: u64, field_name: &'static str) -> Result<i64> {
        i64::try_from(value).map_err(|_| {
            RepositoryError::Database(sea_orm::DbErr::Custom(format!(
                "{field_name} exceeds i64::MAX"
            )))
        })
    }

    fn u64_to_u32(value: u64, field_name: &'static str) -> Result<u32> {
        u32::try_from(value).map_err(|_| {
            RepositoryError::Database(sea_orm::DbErr::Custom(format!(
                "{field_name} exceeds u32::MAX"
            )))
        })
    }

    fn i64_to_u64(value: i64) -> Option<u64> {
        u64::try_from(value).ok()
    }
}
