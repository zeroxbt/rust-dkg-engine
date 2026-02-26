use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{
        big_integer, big_integer_null, big_unsigned, blob_null, string, string_null, unsigned_null,
    },
    sea_query,
};

#[derive(Iden)]
enum KcChainStateMetadata {
    Table,
    BlockchainId,
    ContractAddress,
    KcId,
    RangeStartTokenId,
    RangeEndTokenId,
    BurnedMode,
    BurnedPayload,
    EndEpoch,
    LatestMerkleRoot,
    StateObservedBlock,
    StateUpdatedAt,
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
                    .table(KcChainStateMetadata::Table)
                    .if_not_exists()
                    .col(string(KcChainStateMetadata::BlockchainId))
                    .col(string(KcChainStateMetadata::ContractAddress))
                    .col(big_unsigned(KcChainStateMetadata::KcId))
                    .col(big_integer_null(KcChainStateMetadata::RangeStartTokenId))
                    .col(big_integer_null(KcChainStateMetadata::RangeEndTokenId))
                    .col(unsigned_null(KcChainStateMetadata::BurnedMode))
                    .col(blob_null(KcChainStateMetadata::BurnedPayload))
                    .col(big_integer_null(KcChainStateMetadata::EndEpoch))
                    .col(string_null(KcChainStateMetadata::LatestMerkleRoot))
                    .col(big_integer_null(KcChainStateMetadata::StateObservedBlock))
                    .col(big_integer(KcChainStateMetadata::StateUpdatedAt).default(0))
                    .col(unsigned_null(KcChainStateMetadata::PrivateGraphMode))
                    .col(blob_null(KcChainStateMetadata::PrivateGraphPayload))
                    .col(string_null(KcChainStateMetadata::Source))
                    .col(big_integer(KcChainStateMetadata::CreatedAt))
                    .col(big_integer(KcChainStateMetadata::UpdatedAt))
                    .primary_key(
                        Index::create()
                            .col(KcChainStateMetadata::BlockchainId)
                            .col(KcChainStateMetadata::ContractAddress)
                            .col(KcChainStateMetadata::KcId),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(KcChainStateMetadata::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
