use sea_orm::DeriveMigrationName;
use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, Index, MigrationTrait, SchemaManager, Table},
    schema::*,
};

use crate::models::signatures;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(timestamps(
                Table::create()
                    .table(signatures::Entity)
                    .if_not_exists()
                    .col(pk_auto(signatures::Column::Id))
                    .col(string(signatures::Column::OperationId))
                    .col(string(signatures::Column::SignatureType))
                    .col(string(signatures::Column::IdentityId))
                    .col(tiny_unsigned(signatures::Column::V))
                    .col(string(signatures::Column::R))
                    .col(string(signatures::Column::S))
                    .col(string(signatures::Column::Vs))
                    .to_owned(),
            ))
            .await?;

        // Create index on operation_id for fast lookups
        manager
            .create_index(
                Index::create()
                    .name("idx_signatures_operation_id")
                    .table(signatures::Entity)
                    .col(signatures::Column::OperationId)
                    .to_owned(),
            )
            .await?;

        // Create unique constraint to prevent duplicate signatures
        manager
            .create_index(
                Index::create()
                    .name("idx_signatures_unique")
                    .table(signatures::Entity)
                    .col(signatures::Column::OperationId)
                    .col(signatures::Column::SignatureType)
                    .col(signatures::Column::IdentityId)
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(signatures::Entity)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
