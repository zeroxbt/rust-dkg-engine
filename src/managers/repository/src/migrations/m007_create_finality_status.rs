use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::*,
    sea_query,
};

#[derive(Iden)]
enum FinalityStatus {
    Table,
    Id,
    OperationId,
    Ual,
    PeerId,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(timestamps(
                Table::create()
                    .table(FinalityStatus::Table)
                    .if_not_exists()
                    .col(pk_auto(FinalityStatus::Id))
                    .col(string_len(FinalityStatus::OperationId, 36))
                    .col(string(FinalityStatus::Ual))
                    .col(string(FinalityStatus::PeerId))
                    .to_owned(),
            ))
            .await?;

        // Index on operation_id for lookups
        manager
            .create_index(
                Index::create()
                    .name("idx_finality_status_operation_id")
                    .table(FinalityStatus::Table)
                    .col(FinalityStatus::OperationId)
                    .to_owned(),
            )
            .await?;

        // Index on ual for counting acks
        manager
            .create_index(
                Index::create()
                    .name("idx_finality_status_ual")
                    .table(FinalityStatus::Table)
                    .col(FinalityStatus::Ual)
                    .to_owned(),
            )
            .await?;

        // Unique constraint on (ual, peer_id) - each peer can only ack once per UAL
        manager
            .create_index(
                Index::create()
                    .name("idx_finality_status_unique_ual_peer")
                    .table(FinalityStatus::Table)
                    .col(FinalityStatus::Ual)
                    .col(FinalityStatus::PeerId)
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(FinalityStatus::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
