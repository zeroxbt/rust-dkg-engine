use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, MigrationTrait, SchemaManager, Table},
    schema::*,
    sea_query,
};

#[derive(Iden)]
enum Command {
    Table,
    Id,
    Name,
    Data,
    Sequence,
    ReadyAt,
    Delay,
    StartedAt,
    DeadlineAt,
    Period,
    Status,
    Message,
    ParentId,
    Retries,
    Transactional,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(timestamps(
                Table::create()
                    .table(Command::Table)
                    .if_not_exists()
                    .col(string(Command::Id).primary_key())
                    .col(string(Command::Name))
                    .col(json(Command::Data))
                    .col(json(Command::Sequence))
                    .col(big_integer(Command::ReadyAt))
                    .col(big_integer(Command::Delay).default(0))
                    .col(big_integer(Command::StartedAt))
                    .col(big_integer(Command::DeadlineAt))
                    .col(integer(Command::Period))
                    .col(string(Command::Status))
                    .col(text(Command::Message))
                    .col(string(Command::ParentId))
                    .col(integer(Command::Retries).default(0))
                    .col(boolean(Command::Transactional).default(false))
                    .to_owned(),
            ))
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Command::Table).if_exists().to_owned())
            .await
    }
}
