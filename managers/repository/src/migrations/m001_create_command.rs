use sea_orm::DeriveMigrationName;
use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, MigrationTrait, SchemaManager, Table},
    schema::*,
};

use crate::models::command;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(timestamps(
                Table::create()
                    .table(command::Entity)
                    .if_not_exists()
                    .col(string(command::Column::Id).primary_key())
                    .col(string(command::Column::Name))
                    .col(json(command::Column::Data))
                    .col(json(command::Column::Sequence))
                    .col(big_integer(command::Column::ReadyAt))
                    .col(big_integer(command::Column::Delay).default(0))
                    .col(big_integer(command::Column::StartedAt))
                    .col(big_integer(command::Column::DeadlineAt))
                    .col(integer(command::Column::Period))
                    .col(string(command::Column::Status))
                    .col(text(command::Column::Message))
                    .col(string(command::Column::ParentId))
                    .col(integer(command::Column::Retries).default(0))
                    .col(boolean(command::Column::Transactional).default(false))
                    .to_owned(),
            ))
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(command::Entity).if_exists().to_owned())
            .await
    }
}
