use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{string, string_len, text_null, timestamps},
    sea_query,
};

#[derive(Iden)]
enum OperationStatus {
    Table,
    OperationId,
    OperationName,
    Status,
    ErrorMessage,
    UpdatedAt,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(timestamps(
                Table::create()
                    .table(OperationStatus::Table)
                    .if_not_exists()
                    .col(string_len(OperationStatus::OperationId, 36))
                    .col(string(OperationStatus::OperationName))
                    .col(string(OperationStatus::Status))
                    .col(text_null(OperationStatus::ErrorMessage))
                    .primary_key(Index::create().col(OperationStatus::OperationId))
                    .to_owned(),
            ))
            .await?;

        // Index on status for find_by_status() queries
        manager
            .create_index(
                Index::create()
                    .name("idx_operation_status_status")
                    .table(OperationStatus::Table)
                    .col(OperationStatus::Status)
                    .to_owned(),
            )
            .await?;

        // Composite index for find_by_operation_name_and_status() queries
        manager
            .create_index(
                Index::create()
                    .name("idx_operation_status_name_status")
                    .table(OperationStatus::Table)
                    .col(OperationStatus::OperationName)
                    .col(OperationStatus::Status)
                    .to_owned(),
            )
            .await?;

        // Composite index for cleanup queries filtering by status and updated_at
        manager
            .create_index(
                Index::create()
                    .name("idx_operation_status_status_updated_at")
                    .table(OperationStatus::Table)
                    .col(OperationStatus::Status)
                    .col(OperationStatus::UpdatedAt)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(OperationStatus::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
