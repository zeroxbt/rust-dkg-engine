use std::sync::Arc;

use sea_orm::{
    ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    TransactionTrait,
};

use crate::managers::repository::{
    error::Result,
    models::shard::{ActiveModel, Column, Entity, Model},
};

#[derive(Debug, Clone)]
pub(crate) struct ShardRecordInput {
    pub peer_id: String,
    pub blockchain_id: String,
    pub ask: String,
    pub stake: String,
    pub sha256: String,
}

pub(crate) struct ShardRepository {
    conn: Arc<DatabaseConnection>,
}

impl ShardRepository {
    pub(crate) fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    pub(crate) async fn get_all_peer_records(&self, blockchain_id: &str) -> Result<Vec<Model>> {
        Ok(Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .all(self.conn.as_ref())
            .await?)
    }

    pub(crate) async fn get_peer_record(
        &self,
        blockchain_id: &str,
        peer_id: &str,
    ) -> Result<Option<Model>> {
        Ok(Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .filter(Column::PeerId.eq(peer_id))
            .one(self.conn.as_ref())
            .await?)
    }

    pub(crate) async fn get_peers_count(&self, blockchain_id: &str) -> Result<u64> {
        Ok(Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .count(self.conn.as_ref())
            .await?)
    }

    /// Get all unique peer IDs from the shard table.
    pub(crate) async fn get_all_peer_ids(&self) -> Result<Vec<String>> {
        let result: Vec<Model> = Entity::find().all(self.conn.as_ref()).await?;

        // Deduplicate peer IDs (peers may appear multiple times for different blockchains)
        let mut seen = std::collections::HashSet::new();
        Ok(result
            .into_iter()
            .filter_map(|record| {
                if seen.insert(record.peer_id.clone()) {
                    Some(record.peer_id)
                } else {
                    None
                }
            })
            .collect())
    }

    /// Atomically replaces all sharding table records for a blockchain.
    /// Deletes existing records and inserts new ones within a single transaction.
    pub(crate) async fn replace_sharding_table(
        &self,
        blockchain_id: &str,
        records: Vec<ShardRecordInput>,
    ) -> Result<()> {
        let txn = self.conn.begin().await?;

        Entity::delete_many()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .exec(&txn)
            .await?;

        if !records.is_empty() {
            let active_models: Vec<ActiveModel> = records
                .into_iter()
                .map(|record| ActiveModel {
                    peer_id: ActiveValue::Set(record.peer_id),
                    blockchain_id: ActiveValue::Set(record.blockchain_id),
                    ask: ActiveValue::Set(record.ask),
                    stake: ActiveValue::Set(record.stake),
                    sha256: ActiveValue::Set(record.sha256),
                })
                .collect();

            Entity::insert_many(active_models).exec(&txn).await?;
        }

        txn.commit().await?;
        Ok(())
    }
}
