use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::*,
    sea_query,
};

#[derive(Iden)]
enum Shard {
    Table,
    PeerId,
    BlockchainId,
    Ask,
    Stake,
    LastSeen,
    LastDialed,
    Sha256,
}

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Shard::Table)
                    .if_not_exists()
                    .col(string(Shard::PeerId))
                    .col(string(Shard::BlockchainId))
                    .col(string(Shard::Ask))
                    .col(string(Shard::Stake))
                    .col(date_time(Shard::LastSeen).default("1970-01-01 00:00:00"))
                    .col(date_time(Shard::LastDialed).default("1970-01-01 00:00:00"))
                    .col(string(Shard::Sha256))
                    .primary_key(Index::create().col(Shard::PeerId).col(Shard::BlockchainId))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Shard::Table).if_exists().to_owned())
            .await
    }
}
