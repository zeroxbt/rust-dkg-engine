use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{big_integer, big_unsigned, string, string_null, unsigned_null},
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
                    .col(big_integer(KcChainStateMetadata::RangeStartTokenId))
                    .col(big_integer(KcChainStateMetadata::RangeEndTokenId))
                    .col(sea_query::ColumnDef::new(KcChainStateMetadata::BurnedMode).unsigned().not_null())
                    .col(sea_query::ColumnDef::new(KcChainStateMetadata::BurnedPayload).blob().not_null())
                    .col(big_integer(KcChainStateMetadata::EndEpoch))
                    .col(string(KcChainStateMetadata::LatestMerkleRoot))
                    .col(big_integer(KcChainStateMetadata::StateObservedBlock))
                    .col(big_integer(KcChainStateMetadata::StateUpdatedAt).default(0))
                    .col(unsigned_null(KcChainStateMetadata::PrivateGraphMode))
                    .col(sea_query::ColumnDef::new(KcChainStateMetadata::PrivateGraphPayload).blob())
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
