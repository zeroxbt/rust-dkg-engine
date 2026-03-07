use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use chrono::Utc;
use dkg_observability::record_repository_query;
use sea_orm::{
    ActiveValue, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect,
    sea_query::{Alias, Expr, JoinType, Order, Query},
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

/// Oldest unresolved block range to scan for a contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GapRange {
    pub start_block: u64,
    pub end_block: u64,
    /// Highest KC id expected inside this gap, when known.
    ///
    /// For internal gaps this is `next_known_kc_id - 1`.
    /// Tail gaps do not have a DB-derived bound and return `None`.
    pub last_expected_kc_id: Option<u64>,
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
        publisher_address: &str,
        block_number: u64,
        transaction_hash: &str,
        block_timestamp: u64,
        publish_operation_id: &str,
        source: &str,
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

    /// Upsert canonical KC core metadata sourced from chain events or discovery.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_core_metadata(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_id: u64,
        publisher_address: &str,
        block_number: u64,
        transaction_hash: &str,
        block_timestamp: u64,
        publish_operation_id: &str,
        source: &str,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let block_number = Self::u64_to_i64(block_number, "block_number")?;
        let block_timestamp = Self::u64_to_i64(block_timestamp, "block_timestamp")?;

        let model = CoreActiveModel {
            blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
            contract_address: ActiveValue::Set(contract_address.to_string()),
            kc_id: ActiveValue::Set(kc_id),
            publisher_address: ActiveValue::Set(publisher_address.to_string()),
            block_number: ActiveValue::Set(block_number),
            transaction_hash: ActiveValue::Set(transaction_hash.to_string()),
            block_timestamp: ActiveValue::Set(block_timestamp),
            publish_operation_id: ActiveValue::Set(publish_operation_id.to_string()),
            source: ActiveValue::Set(source.to_string()),
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
                .all(self.conn.as_ref())
                .await?;
            let core_by_id: HashMap<u64, CoreModel> =
                core_rows.into_iter().map(|row| (row.kc_id, row)).collect();

            let state_rows = StateEntity::find()
                .filter(StateColumn::BlockchainId.eq(blockchain_id))
                .filter(StateColumn::ContractAddress.eq(contract_address))
                .filter(StateColumn::KcId.is_in(kc_ids.to_vec()))
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

    /// Return complete metadata + state rows for a mixed (contract_address, kc_id) key set.
    pub async fn get_many_ready_with_kc_state_metadata_for_keys(
        &self,
        blockchain_id: &str,
        keys: &[(String, u64)],
    ) -> Result<HashMap<(String, u64), KcChainReadyKcStateMetadataEntry>> {
        let started = Instant::now();
        if keys.is_empty() {
            record_repository_query(
                "kc_chain_metadata",
                "get_many_ready_with_kc_state_metadata_for_keys",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(HashMap::new());
        }

        let key_set: HashSet<(String, u64)> = keys.iter().cloned().collect();
        let mut contract_addresses: HashSet<String> = HashSet::new();
        let mut kc_ids: HashSet<u64> = HashSet::new();
        for (contract_address, kc_id) in &key_set {
            contract_addresses.insert(contract_address.clone());
            kc_ids.insert(*kc_id);
        }
        let contract_addresses: Vec<String> = contract_addresses.into_iter().collect();
        let kc_ids: Vec<u64> = kc_ids.into_iter().collect();

        let result = async {
            let core_rows = CoreEntity::find()
                .filter(CoreColumn::BlockchainId.eq(blockchain_id))
                .filter(CoreColumn::ContractAddress.is_in(contract_addresses.clone()))
                .filter(CoreColumn::KcId.is_in(kc_ids.clone()))
                .all(self.conn.as_ref())
                .await?;
            let core_by_key: HashMap<(String, u64), CoreModel> = core_rows
                .into_iter()
                .filter_map(|row| {
                    let key = (row.contract_address.clone(), row.kc_id);
                    key_set.contains(&key).then_some((key, row))
                })
                .collect();

            let state_rows = StateEntity::find()
                .filter(StateColumn::BlockchainId.eq(blockchain_id))
                .filter(StateColumn::ContractAddress.is_in(contract_addresses))
                .filter(StateColumn::KcId.is_in(kc_ids))
                .all(self.conn.as_ref())
                .await?;

            let mut out = HashMap::with_capacity(state_rows.len());
            for state in state_rows {
                let key = (state.contract_address.clone(), state.kc_id);
                if let Some(core) = core_by_key.get(&key)
                    && let Some(entry) = Self::to_ready_kc_state_metadata_entry(core, &state)
                {
                    out.insert(key, entry);
                }
            }
            Ok(out)
        }
        .await;

        match &result {
            Ok(rows) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "get_many_ready_with_kc_state_metadata_for_keys",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "get_many_ready_with_kc_state_metadata_for_keys",
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

    /// Return ready metadata keys that are missing projection rows.
    ///
    /// A key is considered ready when:
    /// - core metadata row exists
    /// - matching state metadata row exists
    /// - no row exists in `kc_projection_state`
    pub async fn list_ready_keys_missing_projection(
        &self,
        blockchain_id: &str,
        limit: usize,
    ) -> Result<Vec<(String, u64)>> {
        let started = Instant::now();
        if limit == 0 {
            record_repository_query(
                "kc_chain_metadata",
                "list_ready_keys_missing_projection",
                "ok",
                started.elapsed(),
                Some(0),
            );
            return Ok(Vec::new());
        }

        let db = self.conn.as_ref();
        let c = Alias::new("c");
        let s = Alias::new("s");
        let p = Alias::new("p");
        let blockchain = Alias::new("blockchain_id");
        let contract = Alias::new("contract_address");
        let kc_id = Alias::new("kc_id");

        let mut query = Query::select();
        query
            .column((c.clone(), contract.clone()))
            .column((c.clone(), kc_id.clone()))
            .from_as(Alias::new("kc_chain_core_metadata"), c.clone())
            .join_as(
                JoinType::InnerJoin,
                Alias::new("kc_chain_state_metadata"),
                s.clone(),
                Expr::col((s.clone(), blockchain.clone()))
                    .equals((c.clone(), blockchain.clone()))
                    .and(
                        Expr::col((s.clone(), contract.clone()))
                            .equals((c.clone(), contract.clone())),
                    )
                    .and(Expr::col((s.clone(), kc_id.clone())).equals((c.clone(), kc_id.clone()))),
            )
            .join_as(
                JoinType::LeftJoin,
                Alias::new("kc_projection_state"),
                p.clone(),
                Expr::col((p.clone(), blockchain.clone()))
                    .equals((c.clone(), blockchain.clone()))
                    .and(
                        Expr::col((p.clone(), contract.clone()))
                            .equals((c.clone(), contract.clone())),
                    )
                    .and(Expr::col((p.clone(), kc_id.clone())).equals((c.clone(), kc_id.clone()))),
            )
            .and_where(Expr::col((c.clone(), blockchain)).eq(blockchain_id))
            .and_where(Expr::col((p.clone(), kc_id)).is_null())
            .order_by((c.clone(), contract), Order::Asc)
            .order_by((c, Alias::new("kc_id")), Order::Asc)
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
                    "kc_chain_metadata",
                    "list_ready_keys_missing_projection",
                    "ok",
                    started.elapsed(),
                    Some(rows.len()),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "list_ready_keys_missing_projection",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    /// Find the oldest unresolved gap range for a contract at/after the provided cursor.
    ///
    /// Returns at most one range from the bounded historical KC-id domain
    /// `[cursor_kc_id + 1, expected_max_kc_id]`.
    pub async fn find_oldest_gap_range(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        cursor_block: u64,
        expected_max_kc_id: u64,
        target_tip: u64,
        deployment_block: Option<u64>,
    ) -> Result<Option<GapRange>> {
        let started = Instant::now();
        let next_block = cursor_block.saturating_add(1);
        let result = async {
            if next_block > target_tip {
                return Ok(None);
            }

            let cursor_kc_id = self
                .max_kc_id_up_to_block(blockchain_id, contract_address, cursor_block)
                .await?
                .unwrap_or(0);

            // Search KC-id gaps after the cursor KC id, using count-based
            // binary search over the bounded expected domain.
            let search_start_id = cursor_kc_id.saturating_add(1);
            if search_start_id <= expected_max_kc_id
                && self
                    .kc_id_range_has_gap(
                        blockchain_id,
                        contract_address,
                        search_start_id,
                        expected_max_kc_id,
                    )
                    .await?
            {
                let missing_start_id = self
                    .find_first_missing_kc_id(
                        blockchain_id,
                        contract_address,
                        search_start_id,
                        expected_max_kc_id,
                    )
                    .await?
                    .expect("gap existence checked before binary search");

                // First present KC after the missing run (inside bounded domain) determines:
                // - gap end block (its block)
                // - last expected missing KC id (its kc_id - 1)
                if let Some((next_present_kc_id, gap_end_block)) = self
                    .first_present_kc_id_at_or_after(
                        blockchain_id,
                        contract_address,
                        missing_start_id.saturating_add(1),
                        Some(expected_max_kc_id),
                    )
                    .await?
                {
                    let gap_start_block = self
                        .latest_block_before_kc_id(
                            blockchain_id,
                            contract_address,
                            next_present_kc_id,
                        )
                        .await?
                        .or(deployment_block);

                    if let Some(gap_start_block) = gap_start_block
                        && gap_start_block < gap_end_block
                        && gap_end_block >= next_block
                    {
                        return Ok(Some(GapRange {
                            start_block: gap_start_block,
                            end_block: gap_end_block,
                            last_expected_kc_id: Some(next_present_kc_id.saturating_sub(1))
                                .filter(|value| *value > 0),
                        }));
                    }
                } else {
                    // Missing run reaches the bounded expected max KC id. Keep block range
                    // open until pinned tip, but preserve last_expected_kc_id for fast-forward.
                    let gap_start_block = self
                        .latest_block_before_kc_id(
                            blockchain_id,
                            contract_address,
                            missing_start_id,
                        )
                        .await?
                        .or(deployment_block);
                    if let Some(gap_start_block) = gap_start_block
                        && gap_start_block < target_tip
                        && target_tip >= next_block
                    {
                        return Ok(Some(GapRange {
                            start_block: gap_start_block,
                            end_block: target_tip,
                            last_expected_kc_id: Some(expected_max_kc_id)
                                .filter(|value| *value > 0),
                        }));
                    }
                }
            }

            Ok(None)
        }
        .await;

        match &result {
            Ok(Some(_)) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "find_oldest_gap_range",
                    "ok",
                    started.elapsed(),
                    Some(1),
                );
            }
            Ok(None) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "find_oldest_gap_range",
                    "ok",
                    started.elapsed(),
                    Some(0),
                );
            }
            Err(_) => {
                record_repository_query(
                    "kc_chain_metadata",
                    "find_oldest_gap_range",
                    "error",
                    started.elapsed(),
                    None,
                );
            }
        }

        result
    }

    async fn max_kc_id_up_to_block(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        cursor_block: u64,
    ) -> Result<Option<u64>> {
        let cursor_i64 = Self::u64_to_i64(cursor_block, "cursor_block")?;
        let kc_id = CoreEntity::find()
            .select_only()
            .column(CoreColumn::KcId)
            .filter(CoreColumn::BlockchainId.eq(blockchain_id))
            .filter(CoreColumn::ContractAddress.eq(contract_address))
            .filter(CoreColumn::BlockNumber.lte(cursor_i64))
            .order_by_desc(CoreColumn::KcId)
            .into_tuple::<u64>()
            .one(self.conn.as_ref())
            .await?;

        Ok(kc_id)
    }

    async fn count_kc_ids_in_range(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        start_id: u64,
        end_id: u64,
    ) -> Result<u64> {
        if start_id > end_id {
            return Ok(0);
        }

        let count = CoreEntity::find()
            .filter(CoreColumn::BlockchainId.eq(blockchain_id))
            .filter(CoreColumn::ContractAddress.eq(contract_address))
            .filter(CoreColumn::KcId.between(start_id, end_id))
            .count(self.conn.as_ref())
            .await?;
        Ok(count)
    }

    async fn kc_id_range_has_gap(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        start_id: u64,
        end_id: u64,
    ) -> Result<bool> {
        if start_id > end_id {
            return Ok(false);
        }
        let expected = end_id.saturating_sub(start_id).saturating_add(1);
        let actual = self
            .count_kc_ids_in_range(blockchain_id, contract_address, start_id, end_id)
            .await?;
        Ok(actual < expected)
    }

    async fn find_first_missing_kc_id(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        start_id: u64,
        end_id: u64,
    ) -> Result<Option<u64>> {
        if start_id > end_id {
            return Ok(None);
        }
        if !self
            .kc_id_range_has_gap(blockchain_id, contract_address, start_id, end_id)
            .await?
        {
            return Ok(None);
        }

        let mut lo = start_id;
        let mut hi = end_id;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self
                .kc_id_range_has_gap(blockchain_id, contract_address, start_id, mid)
                .await?
            {
                hi = mid;
            } else {
                lo = mid.saturating_add(1);
            }
        }

        let present_count = self
            .count_kc_ids_in_range(blockchain_id, contract_address, lo, lo)
            .await?;
        if present_count == 0 {
            Ok(Some(lo))
        } else {
            Ok(None)
        }
    }

    async fn first_present_kc_id_at_or_after(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        start_id: u64,
        end_id: Option<u64>,
    ) -> Result<Option<(u64, u64)>> {
        if let Some(end_id) = end_id
            && start_id > end_id
        {
            return Ok(None);
        }

        let mut query = CoreEntity::find()
            .filter(CoreColumn::BlockchainId.eq(blockchain_id))
            .filter(CoreColumn::ContractAddress.eq(contract_address))
            .filter(CoreColumn::KcId.gte(start_id));

        if let Some(end_id) = end_id {
            query = query.filter(CoreColumn::KcId.lte(end_id));
        }

        let row: Option<(u64, i64)> = query
            .select_only()
            .column(CoreColumn::KcId)
            .column(CoreColumn::BlockNumber)
            .order_by_asc(CoreColumn::KcId)
            .into_tuple::<(u64, i64)>()
            .one(self.conn.as_ref())
            .await?;
        Ok(row.map(|(kc_id, block_number)| (kc_id, block_number.max(0) as u64)))
    }

    async fn latest_block_before_kc_id(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        upper_exclusive_kc_id: u64,
    ) -> Result<Option<u64>> {
        if upper_exclusive_kc_id == 0 {
            return Ok(None);
        }

        let row: Option<i64> = CoreEntity::find()
            .select_only()
            .column(CoreColumn::BlockNumber)
            .filter(CoreColumn::BlockchainId.eq(blockchain_id))
            .filter(CoreColumn::ContractAddress.eq(contract_address))
            .filter(CoreColumn::KcId.lt(upper_exclusive_kc_id))
            .order_by_desc(CoreColumn::KcId)
            .into_tuple::<i64>()
            .one(self.conn.as_ref())
            .await?;
        Ok(row.map(|block_number| block_number.max(0) as u64))
    }

    fn to_complete_entry(
        core: CoreModel,
        state: Option<&StateModel>,
    ) -> Option<KcChainMetadataEntry> {
        Some(KcChainMetadataEntry {
            blockchain_id: core.blockchain_id,
            contract_address: core.contract_address,
            kc_id: core.kc_id,
            publisher_address: core.publisher_address,
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
            publisher_address: core.publisher_address.clone(),
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
