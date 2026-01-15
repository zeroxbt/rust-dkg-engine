use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, MigrationTrait, SchemaManager, Table},
    schema::*,
    sea_query,
};

#[derive(Iden)]
enum Operations {
    Table,
    OperationId,
    OperationName,
    Status,
    ErrorMessage,
    Timestamp,
    TotalPeers,
    MinAckResponses,
    CompletedCount,
    FailedCount,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(timestamps(
                Table::create()
                    .table(Operations::Table)
                    .if_not_exists()
                    .col(pk_uuid(Operations::OperationId))
                    .col(string(Operations::OperationName))
                    .col(string_null(Operations::Status))
                    .col(text_null(Operations::ErrorMessage))
                    .col(big_integer(Operations::Timestamp))
                    .col(small_unsigned_null(Operations::TotalPeers))
                    .col(small_unsigned_null(Operations::MinAckResponses))
                    .col(small_unsigned(Operations::CompletedCount).default(0))
                    .col(small_unsigned(Operations::FailedCount).default(0))
                    .to_owned(),
            ))
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(Operations::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
