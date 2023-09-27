use crate::models::commands;
use sea_orm_migration::async_trait::async_trait;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(commands::Entity)
                    .if_not_exists()
                    .col(ColumnDef::new(commands::Column::Id).string().primary_key())
                    .col(ColumnDef::new(commands::Column::Name).string().not_null())
                    .col(ColumnDef::new(commands::Column::Data).json())
                    .col(ColumnDef::new(commands::Column::Sequence).json())
                    .col(ColumnDef::new(commands::Column::ReadyAt).big_integer())
                    .col(
                        ColumnDef::new(commands::Column::Delay)
                            .big_integer()
                            .default(0),
                    )
                    .col(ColumnDef::new(commands::Column::StartedAt).big_integer())
                    .col(ColumnDef::new(commands::Column::DeadlineAt).big_integer())
                    .col(ColumnDef::new(commands::Column::Period).integer())
                    .col(ColumnDef::new(commands::Column::Status).string().not_null())
                    .col(ColumnDef::new(commands::Column::Message).text())
                    .col(ColumnDef::new(commands::Column::ParentId).string())
                    .col(
                        ColumnDef::new(commands::Column::Retries)
                            .integer()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(commands::Column::Transactional)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(commands::Column::CreatedAt)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(commands::Column::UpdatedAt)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(commands::Entity).if_exists().to_owned())
            .await
    }
}
