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
                    .col(boolean(operations::Column::MinAcksReached).default(false))
                    .col(big_integer(operations::Column::Timestamp))
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
