use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::string,
    sea_query,
};

#[derive(Iden)]
enum Shard {
    Table,
    PeerId,
    BlockchainId,
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
                    .primary_key(Index::create().col(Shard::PeerId).col(Shard::BlockchainId))
                    .to_owned(),
            )
            .await?;

        // Index for efficient lookups by blockchain_id
        manager
            .create_index(
                Index::create()
                    .name("idx_shard_blockchain_id")
                    .table(Shard::Table)
                    .col(Shard::BlockchainId)
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
