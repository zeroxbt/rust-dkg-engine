use std::sync::Arc;

use sea_orm::{
    ActiveValue, DatabaseConnection, DbErr, EntityTrait, prelude::DateTimeUtc,
    sea_query::OnConflict,
};

use crate::{
    error::Result,
    models::blockchain::{ActiveModel, Column, Entity},
};

pub struct BlockchainRepository {
    conn: Arc<DatabaseConnection>,
}

impl BlockchainRepository {
    pub fn new(conn: Arc<DatabaseConnection>) -> Self {
        Self { conn }
    }

    pub async fn get_last_checked_block(&self, blockchain_id: &str, contract: &str) -> Result<u64> {
        let model = Entity::find_by_id((blockchain_id.to_owned(), contract.to_owned()))
            .one(self.conn.as_ref())
            .await?;

        let Some(model) = model else {
            return Ok(0);
        };

        Ok(model.last_checked_block.max(0) as u64)
    }

    pub async fn update_last_checked_block(
        &self,
        blockchain_id: &str,
        contract: &str,
        last_checked_block: u64,
        last_checked_timestamp: DateTimeUtc,
    ) -> Result<()> {
        let last_checked_block = i64::try_from(last_checked_block)
            .map_err(|_| DbErr::Custom("last_checked_block exceeds i64::MAX".to_string()))?;

        let model = ActiveModel {
            blockchain_id: ActiveValue::Set(blockchain_id.to_owned()),
            contract: ActiveValue::Set(contract.to_owned()),
            last_checked_block: ActiveValue::Set(last_checked_block),
            last_checked_timestamp: ActiveValue::Set(last_checked_timestamp),
        };
        Entity::insert(model)
            .on_conflict(
                OnConflict::columns([Column::BlockchainId, Column::Contract])
                    .update_columns([Column::LastCheckedBlock, Column::LastCheckedTimestamp])
                    .to_owned(),
            )
            .exec_without_returning(self.conn.as_ref())
            .await?;

        Ok(())
    }
}
