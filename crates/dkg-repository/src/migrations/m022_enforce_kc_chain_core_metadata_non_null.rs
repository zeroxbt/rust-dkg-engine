use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, MigrationTrait, SchemaManager, Table},
    sea_query::{self, ColumnDef},
};

#[derive(Iden)]
enum KcChainCoreMetadata {
    Table,
    PublisherAddress,
    Source,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(KcChainCoreMetadata::Table)
                    .modify_column(
                        ColumnDef::new(KcChainCoreMetadata::PublisherAddress)
                            .string()
                            .not_null(),
                    )
                    .modify_column(
                        ColumnDef::new(KcChainCoreMetadata::Source)
                            .string()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(KcChainCoreMetadata::Table)
                    .modify_column(ColumnDef::new(KcChainCoreMetadata::PublisherAddress).string())
                    .modify_column(ColumnDef::new(KcChainCoreMetadata::Source).string())
                    .to_owned(),
            )
            .await
    }
}
