use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::*,
    sea_query,
};

#[derive(Iden)]
enum Signatures {
    Table,
    Id,
    OperationId,
    IsPublisher,
    IdentityId,
    V,
    R,
    S,
    Vs,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(timestamps(
                Table::create()
                    .table(Signatures::Table)
                    .if_not_exists()
                    .col(pk_auto(Signatures::Id))
                    .col(string(Signatures::OperationId))
                    .col(boolean(Signatures::IsPublisher))
                    .col(string(Signatures::IdentityId))
                    .col(tiny_unsigned(Signatures::V))
                    .col(string(Signatures::R))
                    .col(string(Signatures::S))
                    .col(string(Signatures::Vs))
                    .to_owned(),
            ))
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_signatures_operation_id")
                    .table(Signatures::Table)
                    .col(Signatures::OperationId)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_signatures_unique")
                    .table(Signatures::Table)
                    .col(Signatures::OperationId)
                    .col(Signatures::IsPublisher)
                    .col(Signatures::IdentityId)
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
                    .table(Signatures::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
