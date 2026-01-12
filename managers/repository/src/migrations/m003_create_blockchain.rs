use sea_orm::DeriveMigrationName;
use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, Index, MigrationTrait, SchemaManager, Table},
    schema::*,
};

use crate::models::blockchain;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(blockchain::Entity)
                    .if_not_exists()
                    .col(string(blockchain::Column::BlockchainId))
                    .col(string_len(blockchain::Column::Contract, 42))
                    .col(
                        unsigned(blockchain::Column::LastCheckedBlock)
                            .big_integer()
                            .default("0"),
                    )
                    .col(
                        date_time(blockchain::Column::LastCheckedTimestamp)
                            .default("1970-01-01 00:00:00"),
                    )
                    .primary_key(
                        Index::create()
                            .col(blockchain::Column::BlockchainId)
                            .col(blockchain::Column::Contract),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(blockchain::Entity)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
