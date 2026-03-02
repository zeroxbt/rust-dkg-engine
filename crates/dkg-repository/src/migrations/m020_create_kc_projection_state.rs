use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{big_integer, big_integer_null, big_unsigned, string, string_null},
    sea_query,
};

#[derive(Iden)]
enum KcProjectionState {
    Table,
    BlockchainId,
    ContractAddress,
    KcId,
    DesiredState,
    ActualState,
    LastSyncedAt,
    LastError,
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
                    .table(KcProjectionState::Table)
                    .if_not_exists()
                    .col(string(KcProjectionState::BlockchainId))
                    .col(string(KcProjectionState::ContractAddress))
                    .col(big_unsigned(KcProjectionState::KcId))
                    .col(
                        sea_query::ColumnDef::new(KcProjectionState::DesiredState)
                            .tiny_unsigned()
                            .not_null(),
                    )
                    .col(
                        sea_query::ColumnDef::new(KcProjectionState::ActualState)
                            .tiny_unsigned()
                            .not_null(),
                    )
                    .col(big_integer_null(KcProjectionState::LastSyncedAt))
                    .col(string_null(KcProjectionState::LastError))
                    .col(big_integer(KcProjectionState::CreatedAt))
                    .col(big_integer(KcProjectionState::UpdatedAt))
                    .primary_key(
                        Index::create()
                            .col(KcProjectionState::BlockchainId)
                            .col(KcProjectionState::ContractAddress)
                            .col(KcProjectionState::KcId),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_kc_projection_state_status")
                    .table(KcProjectionState::Table)
                    .col(KcProjectionState::BlockchainId)
                    .col(KcProjectionState::DesiredState)
                    .col(KcProjectionState::ActualState)
                    .col(KcProjectionState::UpdatedAt)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_kc_projection_state_status")
                    .table(KcProjectionState::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(
                Table::drop()
                    .table(KcProjectionState::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
