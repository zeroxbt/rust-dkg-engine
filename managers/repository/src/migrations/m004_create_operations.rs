use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
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
                    .col(string(Operations::Status))
                    .col(text_null(Operations::ErrorMessage))
                    .col(big_integer(Operations::Timestamp))
                    .col(small_unsigned_null(Operations::TotalPeers))
                    .col(small_unsigned_null(Operations::MinAckResponses))
                    .col(small_unsigned(Operations::CompletedCount).default(0))
                    .col(small_unsigned(Operations::FailedCount).default(0))
                    .to_owned(),
            ))
            .await?;

        // Index on status for find_by_status() queries
        manager
            .create_index(
                Index::create()
                    .name("idx_operations_status")
                    .table(Operations::Table)
                    .col(Operations::Status)
                    .to_owned(),
            )
            .await?;

        // Composite index for find_by_operation_name_and_status() queries
        manager
            .create_index(
                Index::create()
                    .name("idx_operations_name_status")
                    .table(Operations::Table)
                    .col(Operations::OperationName)
                    .col(Operations::Status)
                    .to_owned(),
            )
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
