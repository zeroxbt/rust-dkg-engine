use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{big_integer, string},
    sea_query,
};

#[derive(Iden)]
enum KcSyncMetadataCursor {
    Table,
    BlockchainId,
    ContractAddress,
    LastCheckedBlock,
    UpdatedAt,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(KcSyncMetadataCursor::Table)
                    .if_not_exists()
                    .col(string(KcSyncMetadataCursor::BlockchainId))
                    .col(string(KcSyncMetadataCursor::ContractAddress))
                    .col(big_integer(KcSyncMetadataCursor::LastCheckedBlock).default(0))
                    .col(big_integer(KcSyncMetadataCursor::UpdatedAt))
                    .primary_key(
                        Index::create()
                            .col(KcSyncMetadataCursor::BlockchainId)
                            .col(KcSyncMetadataCursor::ContractAddress),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(KcSyncMetadataCursor::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
