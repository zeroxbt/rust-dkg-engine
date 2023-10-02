use crate::models::command;
use sea_orm::DeriveMigrationName;
use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{ColumnDef, DbErr, Expr, MigrationTrait, SchemaManager, Table},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(command::Entity)
                    .if_not_exists()
                    .col(ColumnDef::new(command::Column::Id).string().primary_key())
                    .col(ColumnDef::new(command::Column::Name).string().not_null())
                    .col(ColumnDef::new(command::Column::Data).json())
                    .col(ColumnDef::new(command::Column::Sequence).json())
                    .col(ColumnDef::new(command::Column::ReadyAt).big_integer())
                    .col(
                        ColumnDef::new(command::Column::Delay)
                            .big_integer()
                            .default(0),
                    )
                    .col(ColumnDef::new(command::Column::StartedAt).big_integer())
                    .col(ColumnDef::new(command::Column::DeadlineAt).big_integer())
                    .col(ColumnDef::new(command::Column::Period).integer())
                    .col(ColumnDef::new(command::Column::Status).string().not_null())
                    .col(ColumnDef::new(command::Column::Message).text())
                    .col(ColumnDef::new(command::Column::ParentId).string())
                    .col(
                        ColumnDef::new(command::Column::Retries)
                            .integer()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(command::Column::Transactional)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .col(
                        ColumnDef::new(command::Column::CreatedAt)
                            .date_time()
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .col(
                        ColumnDef::new(command::Column::UpdatedAt)
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
            .drop_table(Table::drop().table(command::Entity).if_exists().to_owned())
            .await
    }
}
