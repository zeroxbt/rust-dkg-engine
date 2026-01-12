use sea_orm::DeriveMigrationName;
use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, ForeignKey, ForeignKeyAction, MigrationTrait, SchemaManager, Table},
    schema::*,
};

use crate::models::{operation_responses, operations};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(timestamps(
                Table::create()
                    .table(operation_responses::Entity)
                    .if_not_exists()
                    .col(pk_auto(operation_responses::Column::Id))
                    .col(uuid(operation_responses::Column::OperationId))
                    .col(string_null(operation_responses::Column::NodeId))
                    .col(string(operation_responses::Column::Status))
                    .col(text_null(operation_responses::Column::Message))
                    .col(string_null(operation_responses::Column::Keyword))
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_operation_responses_operation_id")
                            .from(
                                operation_responses::Entity,
                                operation_responses::Column::OperationId,
                            )
                            .to(operations::Entity, operations::Column::OperationId)
                            .on_delete(ForeignKeyAction::Cascade)
                            .on_update(ForeignKeyAction::Cascade),
                    )
                    .to_owned(),
            ))
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(operation_responses::Entity)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
