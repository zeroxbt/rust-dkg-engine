use sea_orm::DeriveMigrationName;
use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, Index, MigrationTrait, SchemaManager, Table},
    schema::*,
};

use crate::models::shard;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(shard::Entity)
                    .if_not_exists()
                    .col(string(shard::Column::PeerId))
                    .col(string(shard::Column::BlockchainId))
                    .col(string(shard::Column::Ask))
                    .col(string(shard::Column::Stake))
                    .col(date_time(shard::Column::LastSeen).default("1970-01-01 00:00:00"))
                    .col(date_time(shard::Column::LastDialed).default("1970-01-01 00:00:00"))
                    .col(string(shard::Column::Sha256))
                    .primary_key(
                        Index::create()
                            .col(shard::Column::PeerId)
                            .col(shard::Column::BlockchainId),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(shard::Entity).if_exists().to_owned())
            .await
    }
}
