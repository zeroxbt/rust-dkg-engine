use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::*,
    sea_query,
};

#[derive(Iden)]
enum Blockchain {
    Table,
    BlockchainId,
    Contract,
    LastCheckedBlock,
    LastCheckedTimestamp,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Blockchain::Table)
                    .if_not_exists()
                    .col(string(Blockchain::BlockchainId))
                    .col(string_len(Blockchain::Contract, 42))
                    .col(
                        unsigned(Blockchain::LastCheckedBlock)
                            .big_integer()
                            .default("0"),
                    )
                    .col(date_time(Blockchain::LastCheckedTimestamp).default("1970-01-01 00:00:00"))
                    .primary_key(
                        Index::create()
                            .col(Blockchain::BlockchainId)
                            .col(Blockchain::Contract),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(Blockchain::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
