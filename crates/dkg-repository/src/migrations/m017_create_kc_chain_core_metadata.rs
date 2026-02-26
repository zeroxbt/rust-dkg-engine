use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{big_integer, big_unsigned, string, string_null},
    sea_query,
};

#[derive(Iden)]
enum KcChainCoreMetadata {
    Table,
    BlockchainId,
    ContractAddress,
    KcId,
    PublisherAddress,
    BlockNumber,
    TransactionHash,
    BlockTimestamp,
    PublishOperationId,
    Source,
    CreatedAt,
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
                    .table(KcChainCoreMetadata::Table)
                    .if_not_exists()
                    .col(string(KcChainCoreMetadata::BlockchainId))
                    .col(string(KcChainCoreMetadata::ContractAddress))
                    .col(big_unsigned(KcChainCoreMetadata::KcId))
                    .col(string_null(KcChainCoreMetadata::PublisherAddress))
                    .col(big_integer(KcChainCoreMetadata::BlockNumber))
                    .col(string(KcChainCoreMetadata::TransactionHash))
                    .col(big_integer(KcChainCoreMetadata::BlockTimestamp))
                    .col(string(KcChainCoreMetadata::PublishOperationId))
                    .col(string_null(KcChainCoreMetadata::Source))
                    .col(big_integer(KcChainCoreMetadata::CreatedAt))
                    .col(big_integer(KcChainCoreMetadata::UpdatedAt))
                    .primary_key(
                        Index::create()
                            .col(KcChainCoreMetadata::BlockchainId)
                            .col(KcChainCoreMetadata::ContractAddress)
                            .col(KcChainCoreMetadata::KcId),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(KcChainCoreMetadata::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
