use crate::models::shard;
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
                    .table(shard::Entity)
                    .if_not_exists()
                    .col(ColumnDef::new(shard::Column::PeerId).string())
                    .col(ColumnDef::new(shard::Column::BlockchainId).string())
                    .col(ColumnDef::new(shard::Column::Ask).string().not_null())
                    .col(ColumnDef::new(shard::Column::Stake).string().not_null())
                    .col(
                        ColumnDef::new(shard::Column::LastSeen)
                            .date_time()
                            .not_null()
                            .default("1970-01-01 00:00:00"),
                    )
                    .col(
                        ColumnDef::new(shard::Column::LastDialed)
                            .date_time()
                            .not_null()
                            .default("1970-01-01 00:00:00"),
                    )
                    .col(ColumnDef::new(shard::Column::Sha256).string().not_null())
                    .primary_key(
                        Index::create()
                            .col(shard::Column::PeerId)
                            .col(shard::Column::BlockchainId),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(shard::Entity).if_exists().to_owned())
            .await
    }
}
