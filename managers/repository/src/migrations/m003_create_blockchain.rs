use crate::models::blockchain;
use sea_orm::DeriveMigrationName;
use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{ColumnDef, DbErr, Index, MigrationTrait, SchemaManager, Table},
};

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
                    .col(ColumnDef::new(blockchain::Column::BlockchainId).string())
                    .col(ColumnDef::new(blockchain::Column::Contract).string_len(42))
                    .col(
                        ColumnDef::new(blockchain::Column::LastCheckedBlock)
                            .big_integer()
                            .unsigned()
                            .not_null()
                            .default("0"),
                    )
                    .col(
                        ColumnDef::new(blockchain::Column::LastCheckedTimestamp)
                            .date_time()
                            .not_null()
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
