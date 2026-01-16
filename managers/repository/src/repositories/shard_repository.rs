use std::sync::Arc;

use chrono::Duration;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, DbBackend, EntityTrait,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Statement, TransactionTrait,
    UpdateResult, error::DbErr, prelude::DateTimeUtc, sea_query::Expr,
};

use crate::models::shard::{ActiveModel, Column, Entity, Model};

#[derive(Debug, Clone)]
pub struct ShardRecordInput {
    pub peer_id: String,
    pub blockchain_id: String,
    pub ask: String,
    pub stake: String,
    pub sha256: String,
}

pub struct ShardRepository {
    conn: Arc<DatabaseConnection>,
}

impl ShardRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    pub async fn create_many_peer_records(
        &self,
        records: Vec<ShardRecordInput>,
    ) -> Result<(), DbErr> {
        let active_models: Vec<ActiveModel> = records
            .into_iter()
            .map(|record| ActiveModel {
                peer_id: ActiveValue::Set(record.peer_id),
                blockchain_id: ActiveValue::Set(record.blockchain_id),
                ask: ActiveValue::Set(record.ask),
                stake: ActiveValue::Set(record.stake),
                sha256: ActiveValue::Set(record.sha256),
                last_seen: ActiveValue::NotSet,
                last_dialed: ActiveValue::NotSet,
            })
            .collect();

        Entity::insert_many(active_models)
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    pub async fn remove_sharding_table_peer_records(
        &self,
        blockchain_id: &str,
    ) -> Result<(), DbErr> {
        Entity::delete_many()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    pub async fn remove_peer_record(
        &self,
        blockchain_id: &str,
        peer_id: &str,
    ) -> Result<(), DbErr> {
        Entity::delete_by_id((blockchain_id.to_owned(), peer_id.to_owned()))
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    pub async fn create_peer_record(&self, record: ShardRecordInput) -> Result<(), DbErr> {
        let active_model = ActiveModel {
            peer_id: ActiveValue::Set(record.peer_id),
            blockchain_id: ActiveValue::Set(record.blockchain_id),
            ask: ActiveValue::Set(record.ask),
            stake: ActiveValue::Set(record.stake),
            sha256: ActiveValue::Set(record.sha256),
            last_seen: ActiveValue::NotSet,
            last_dialed: ActiveValue::NotSet,
        };

        Entity::insert(active_model)
            .exec(self.conn.as_ref())
            .await?;

        Ok(())
    }

    pub async fn get_all_peer_records(
        &self,
        blockchain_id: &str,
        filter_last_seen: bool,
    ) -> Result<Vec<Model>, DbErr> {
        let query = if filter_last_seen {
            r#"SELECT * FROM shard WHERE blockchain_id = ? and last_seen >= last_dialed"#
        } else {
            r#"SELECT * FROM shard WHERE blockchain_id = ?"#
        };

        Entity::find()
            .from_raw_sql(Statement::from_sql_and_values(
                DbBackend::MySql,
                query,
                [blockchain_id.into()],
            ))
            .all(self.conn.as_ref())
            .await
    }

    pub async fn get_peer_record(
        &self,
        blockchain_id: &str,
        peer_id: &str,
    ) -> Result<Option<Model>, DbErr> {
        Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .filter(Column::PeerId.eq(peer_id))
            .one(self.conn.as_ref())
            .await
    }

    pub async fn get_peers_count(&self, blockchain_id: &str) -> Result<u64, DbErr> {
        Entity::find()
            .filter(Column::BlockchainId.eq(blockchain_id))
            .count(self.conn.as_ref())
            .await
    }

    pub async fn get_peers_to_dial(
        &self,
        limit: usize,
        dial_frequency_millis: i64,
    ) -> Result<Vec<String>, DbErr> {
        let earlier_time = chrono::Utc::now() - Duration::milliseconds(dial_frequency_millis);

        let result: Vec<Model> = Entity::find()
            .filter(Column::LastDialed.lt(earlier_time))
            .order_by(Column::LastDialed, sea_orm::Order::Asc)
            .limit(limit as u64)
            .all(self.conn.as_ref())
            .await?;

        Ok(result.into_iter().map(|record| record.peer_id).collect())
    }

    pub async fn update_peer_ask(
        &self,
        blockchain_id: String,
        peer_id: String,
        ask: String,
    ) -> Result<(), DbErr> {
        let result = ActiveModel {
            peer_id: ActiveValue::Set(peer_id),
            blockchain_id: ActiveValue::Set(blockchain_id),
            ask: ActiveValue::Set(ask),
            ..Default::default()
        }
        .update(self.conn.as_ref())
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(DbErr::RecordNotUpdated) => Ok(()),
            Err(other) => Err(other),
        }
    }

    pub async fn update_peer_stake(
        &self,
        blockchain_id: String,
        peer_id: String,
        stake: String,
    ) -> Result<(), DbErr> {
        let result = ActiveModel {
            peer_id: ActiveValue::Set(peer_id),
            blockchain_id: ActiveValue::Set(blockchain_id),
            stake: ActiveValue::Set(stake),
            ..Default::default()
        }
        .update(self.conn.as_ref())
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(DbErr::RecordNotUpdated) => Ok(()),
            Err(other) => Err(other),
        }
    }

    pub async fn update_peer_record_last_dialed(
        &self,
        peer_id: String,
        timestamp: DateTimeUtc,
    ) -> Result<(), DbErr> {
        ActiveModel {
            peer_id: ActiveValue::Set(peer_id),
            last_dialed: ActiveValue::Set(timestamp),
            ..Default::default()
        }
        .update(self.conn.as_ref())
        .await?;

        Ok(())
    }

    pub async fn update_peer_record_last_seen_and_last_dialed(
        &self,
        peer_id: String,
        timestamp: DateTimeUtc,
    ) -> Result<UpdateResult, DbErr> {
        Entity::update_many()
            .col_expr(Column::LastSeen, Expr::value::<DateTimeUtc>(timestamp))
            .col_expr(Column::LastDialed, Expr::value::<DateTimeUtc>(timestamp))
            .filter(Column::PeerId.eq(peer_id))
            .exec(self.conn.as_ref())
            .await
    }

    pub async fn clean_sharding_table(&self, blockchain_id: Option<String>) -> Result<(), DbErr> {
        if let Some(id) = blockchain_id {
            Entity::delete_many()
                .filter(Column::BlockchainId.eq(id))
                .exec(self.conn.as_ref())
                .await?;
        } else {
            Entity::delete_many().exec(self.conn.as_ref()).await?;
        }
        Ok(())
    }

    /// Atomically replaces all sharding table records for a blockchain.
    /// Deletes existing records and inserts new ones within a single transaction.
    pub async fn replace_sharding_table(
        &self,
        blockchain_id: &str,
        records: Vec<ShardRecordInput>,
    ) -> Result<(), DbErr> {
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
                    last_seen: ActiveValue::NotSet,
                    last_dialed: ActiveValue::NotSet,
                })
                .collect();

            Entity::insert_many(active_models).exec(&txn).await?;
        }

        txn.commit().await?;
        Ok(())
    }
}
