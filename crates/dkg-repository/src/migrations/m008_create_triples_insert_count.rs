use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, MigrationTrait, SchemaManager, Table},
    schema::{big_integer, pk_auto},
    sea_query,
};

#[derive(Iden)]
enum TriplesInsertCount {
    Table,
    Id,
    Count,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(TriplesInsertCount::Table)
                    .if_not_exists()
                    .col(pk_auto(TriplesInsertCount::Id))
                    .col(big_integer(TriplesInsertCount::Count).default(0))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(TriplesInsertCount::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
