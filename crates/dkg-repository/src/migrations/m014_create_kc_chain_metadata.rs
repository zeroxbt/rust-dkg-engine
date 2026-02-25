use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{
        big_integer, big_integer_null, big_unsigned, blob_null, string, string_null, unsigned_null,
    },
    sea_query,
};

#[derive(Iden)]
enum KcChainMetadata {
    Table,
    BlockchainId,
    ContractAddress,
    KcId,
    PublisherAddress,
    BlockNumber,
    TransactionHash,
    BlockTimestamp,
    PublishOperationId,
    PrivateGraphMode,
    PrivateGraphPayload,
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
                    .table(KcChainMetadata::Table)
                    .if_not_exists()
                    .col(string(KcChainMetadata::BlockchainId))
                    .col(string(KcChainMetadata::ContractAddress))
                    .col(big_unsigned(KcChainMetadata::KcId))
                    .col(string_null(KcChainMetadata::PublisherAddress))
                    .col(big_integer_null(KcChainMetadata::BlockNumber))
                    .col(string_null(KcChainMetadata::TransactionHash))
                    .col(big_integer_null(KcChainMetadata::BlockTimestamp))
                    .col(string_null(KcChainMetadata::PublishOperationId))
                    .col(unsigned_null(KcChainMetadata::PrivateGraphMode))
                    .col(blob_null(KcChainMetadata::PrivateGraphPayload))
                    .col(string_null(KcChainMetadata::Source))
                    .col(big_integer(KcChainMetadata::CreatedAt))
                    .col(big_integer(KcChainMetadata::UpdatedAt))
                    .primary_key(
                        Index::create()
                            .col(KcChainMetadata::BlockchainId)
                            .col(KcChainMetadata::ContractAddress)
                            .col(KcChainMetadata::KcId),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_kc_chain_metadata_lookup")
                    .table(KcChainMetadata::Table)
                    .col(KcChainMetadata::BlockchainId)
                    .col(KcChainMetadata::ContractAddress)
                    .col(KcChainMetadata::KcId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(KcChainMetadata::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
