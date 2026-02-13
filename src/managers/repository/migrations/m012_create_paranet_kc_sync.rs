use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{big_integer, string, string_len, string_null, unsigned},
    sea_query,
};

#[derive(Iden)]
enum ParanetKcSync {
    Table,
    ParanetUal,
    KcUal,
    BlockchainId,
    ParanetId,
    RetryCount,
    NextRetryAt,
    LastError,
    Status,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(ParanetKcSync::Table)
                    .if_not_exists()
                    .col(string(ParanetKcSync::ParanetUal))
                    .col(string(ParanetKcSync::KcUal))
                    .col(string(ParanetKcSync::BlockchainId))
                    .col(string(ParanetKcSync::ParanetId))
                    .col(unsigned(ParanetKcSync::RetryCount).default(0))
                    .col(big_integer(ParanetKcSync::NextRetryAt).default(0))
                    .col(string_null(ParanetKcSync::LastError))
                    .col(string_len(ParanetKcSync::Status, 16).default("pending"))
                    .col(big_integer(ParanetKcSync::CreatedAt))
                    .col(big_integer(ParanetKcSync::UpdatedAt))
                    .primary_key(
                        Index::create()
                            .col(ParanetKcSync::ParanetUal)
                            .col(ParanetKcSync::KcUal),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_paranet_kc_sync_due")
                    .table(ParanetKcSync::Table)
                    .col(ParanetKcSync::BlockchainId)
                    .col(ParanetKcSync::ParanetUal)
                    .col(ParanetKcSync::Status)
                    .col(ParanetKcSync::NextRetryAt)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_paranet_kc_sync_status")
                    .table(ParanetKcSync::Table)
                    .col(ParanetKcSync::ParanetUal)
                    .col(ParanetKcSync::Status)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(ParanetKcSync::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
