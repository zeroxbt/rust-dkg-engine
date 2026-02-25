use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use chrono::Utc;
use sea_orm::{ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};

use crate::{
    error::{RepositoryError, Result},
    models::kc_chain_metadata::{ActiveModel, Column, Entity, Model},
    types::KcChainMetadataEntry,
};

#[derive(Clone)]
pub struct KcChainMetadataRepository {
    conn: Arc<DatabaseConnection>,
}

impl KcChainMetadataRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    /// Upsert canonical KC metadata sourced from chain events or backfill.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_id: u64,
        publisher_address: Option<&str>,
        block_number: Option<u64>,
        transaction_hash: Option<&str>,
        block_timestamp: Option<u64>,
        publish_operation_id: Option<&str>,
        source: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let block_number = block_number.map(i64::try_from).transpose().map_err(|_| {
            RepositoryError::Database(sea_orm::DbErr::Custom(
                "block_number exceeds i64::MAX".to_string(),
            ))
        })?;
        let block_timestamp = block_timestamp
            .map(i64::try_from)
            .transpose()
            .map_err(|_| {
                RepositoryError::Database(sea_orm::DbErr::Custom(
                    "block_timestamp exceeds i64::MAX".to_string(),
                ))
            })?;

        let model = ActiveModel {
            blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
            contract_address: ActiveValue::Set(contract_address.to_string()),
            kc_id: ActiveValue::Set(kc_id),
            publisher_address: ActiveValue::Set(publisher_address.map(ToString::to_string)),
            block_number: ActiveValue::Set(block_number),
            transaction_hash: ActiveValue::Set(transaction_hash.map(ToString::to_string)),
            block_timestamp: ActiveValue::Set(block_timestamp),
            private_graph_mode: ActiveValue::Set(None),
            private_graph_payload: ActiveValue::Set(None),
            publish_operation_id: ActiveValue::Set(publish_operation_id.map(ToString::to_string)),
            source: ActiveValue::Set(source.map(ToString::to_string)),
            created_at: ActiveValue::Set(now),
            updated_at: ActiveValue::Set(now),
        };

        Entity::insert(model)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([
                    Column::BlockchainId,
                    Column::ContractAddress,
                    Column::KcId,
                ])
                .update_columns([
                    Column::PublisherAddress,
                    Column::BlockNumber,
                    Column::TransactionHash,
                    Column::BlockTimestamp,
                    Column::PublishOperationId,
                    Column::Source,
                    Column::UpdatedAt,
                ])
                .to_owned(),
            )
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    /// Upsert private graph encoding for a KC without mutating canonical chain metadata fields.
    ///
    /// This is used by triple-store insert paths that can derive public/private graph presence
    /// while chain metadata may be unavailable at that moment.
    pub async fn upsert_private_graph_encoding(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_id: u64,
        private_graph_mode: Option<u32>,
        private_graph_payload: Option<&[u8]>,
        source: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().timestamp();

        let model = ActiveModel {
            blockchain_id: ActiveValue::Set(blockchain_id.to_string()),
            contract_address: ActiveValue::Set(contract_address.to_string()),
            kc_id: ActiveValue::Set(kc_id),
            publisher_address: ActiveValue::Set(None),
            block_number: ActiveValue::Set(None),
            transaction_hash: ActiveValue::Set(None),
            block_timestamp: ActiveValue::Set(None),
            private_graph_mode: ActiveValue::Set(private_graph_mode),
            private_graph_payload: ActiveValue::Set(private_graph_payload.map(ToOwned::to_owned)),
            publish_operation_id: ActiveValue::Set(None),
            source: ActiveValue::Set(source.map(ToString::to_string)),
            created_at: ActiveValue::Set(now),
            updated_at: ActiveValue::Set(now),
        };

        Entity::insert(model)
            .on_conflict(
                sea_orm::sea_query::OnConflict::columns([
                    Column::BlockchainId,
                    Column::ContractAddress,
                    Column::KcId,
                ])
                .update_columns([
                    Column::PrivateGraphMode,
                    Column::PrivateGraphPayload,
                    Column::Source,
                    Column::UpdatedAt,
                ])
                .to_owned(),
            )
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    /// Get complete metadata for a single KC.
    pub async fn get_complete(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_id: u64,
    ) -> Result<Option<KcChainMetadataEntry>> {
        let row = Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .filter(Column::ContractAddress.eq(contract_address))
            .filter(Column::KcId.eq(kc_id))
            .filter(Column::PublisherAddress.is_not_null())
            .filter(Column::BlockNumber.is_not_null())
            .filter(Column::TransactionHash.is_not_null())
            .filter(Column::BlockTimestamp.is_not_null())
            .one(self.conn.as_ref())
            .await?;

        Ok(row.and_then(Self::to_complete_entry))
    }

    /// Return complete metadata rows for a KC id set.
    pub async fn get_many_complete(
        &self,
        blockchain_id: &str,
        contract_address: &str,
        kc_ids: &[u64],
    ) -> Result<HashMap<u64, KcChainMetadataEntry>> {
        if kc_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let rows = Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .filter(Column::ContractAddress.eq(contract_address))
            .filter(Column::KcId.is_in(kc_ids.to_vec()))
            .filter(Column::PublisherAddress.is_not_null())
            .filter(Column::BlockNumber.is_not_null())
            .filter(Column::TransactionHash.is_not_null())
            .filter(Column::BlockTimestamp.is_not_null())
            .all(self.conn.as_ref())
            .await?;

        let mut out = HashMap::with_capacity(rows.len());
        for row in rows {
            if let Some(entry) = Self::to_complete_entry(row) {
                out.insert(entry.kc_id, entry);
            }
        }
        Ok(out)
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

    fn to_complete_entry(model: Model) -> Option<KcChainMetadataEntry> {
        Some(KcChainMetadataEntry {
            blockchain_id: model.blockchain_id,
            contract_address: model.contract_address,
            kc_id: model.kc_id,
            publisher_address: model.publisher_address?,
            block_number: u64::try_from(model.block_number?).ok()?,
            transaction_hash: model.transaction_hash?,
            block_timestamp: u64::try_from(model.block_timestamp?).ok()?,
            private_graph_mode: model.private_graph_mode,
            private_graph_payload: model.private_graph_payload,
            publish_operation_id: model.publish_operation_id,
            source: model.source,
            created_at: model.created_at,
            updated_at: model.updated_at,
        })
    }
}
