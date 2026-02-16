use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::big_integer,
    sea_query,
};

#[derive(Iden)]
enum KcSyncQueue {
    Table,
    BlockchainId,
    ContractAddress,
    KcId,
    RetryCount,
    NextRetryAt,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(KcSyncQueue::Table)
                    .add_column(big_integer(KcSyncQueue::NextRetryAt).default(0))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx_kc_sync_queue_due")
                    .table(KcSyncQueue::Table)
                    .col(KcSyncQueue::BlockchainId)
                    .col(KcSyncQueue::ContractAddress)
                    .col(KcSyncQueue::RetryCount)
                    .col(KcSyncQueue::NextRetryAt)
                    .col(KcSyncQueue::KcId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_kc_sync_queue_due")
                    .table(KcSyncQueue::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(KcSyncQueue::Table)
                    .drop_column(KcSyncQueue::NextRetryAt)
                    .to_owned(),
            )
            .await
    }
}
