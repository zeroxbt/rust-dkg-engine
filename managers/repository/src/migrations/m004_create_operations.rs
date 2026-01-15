use sea_orm::DeriveMigrationName;
use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, MigrationTrait, SchemaManager, Table},
    schema::*,
};

use crate::models::operations;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(timestamps(
                Table::create()
                    .table(operations::Entity)
                    .if_not_exists()
                    .col(pk_uuid(operations::Column::OperationId))
                    .col(string(operations::Column::OperationName))
                    .col(string_null(operations::Column::Status))
                    .col(text_null(operations::Column::ErrorMessage))
                    .col(big_integer(operations::Column::Timestamp))
                    // Progress tracking fields
                    .col(small_unsigned_null(operations::Column::TotalPeers))
                    .col(small_unsigned_null(operations::Column::MinAckResponses))
                    .col(small_unsigned(operations::Column::CompletedCount).default(0))
                    .col(small_unsigned(operations::Column::FailedCount).default(0))
                    .to_owned(),
            ))
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(operations::Entity)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
