use sea_orm::DeriveMigrationName;
use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{ColumnDef, DbErr, Expr, Index, MigrationTrait, SchemaManager, Table},
};

use crate::models::operation_ids;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(operation_ids::Entity)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(operation_ids::Column::OperationId)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(operation_ids::Column::Data).text().null())
                    .col(
                        ColumnDef::new(operation_ids::Column::Status)
                            .string()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(operation_ids::Column::Timestamp)
                            .big_integer()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(operation_ids::Column::CreatedAt)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(operation_ids::Column::UpdatedAt)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await?;

        // Create index on status for faster queries
        manager
            .create_index(
                Index::create()
                    .name("idx_operation_ids_status")
                    .table(operation_ids::Entity)
                    .col(operation_ids::Column::Status)
                    .to_owned(),
            )
            .await?;

        // Create index on timestamp for faster range queries
        manager
            .create_index(
                Index::create()
                    .name("idx_operation_ids_timestamp")
                    .table(operation_ids::Entity)
                    .col(operation_ids::Column::Timestamp)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(operation_ids::Entity)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
