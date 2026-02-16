use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{big_integer, big_unsigned, string},
    sea_query,
};

#[derive(Iden)]
enum KcSyncProgress {
    Table,
    BlockchainId,
    ContractAddress,
    LastCheckedId,
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
                    .table(KcSyncProgress::Table)
                    .if_not_exists()
                    .col(string(KcSyncProgress::BlockchainId))
                    .col(string(KcSyncProgress::ContractAddress))
                    .col(big_unsigned(KcSyncProgress::LastCheckedId).default(0))
                    .col(big_integer(KcSyncProgress::UpdatedAt))
                    .primary_key(
                        Index::create()
                            .col(KcSyncProgress::BlockchainId)
                            .col(KcSyncProgress::ContractAddress),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(KcSyncProgress::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
