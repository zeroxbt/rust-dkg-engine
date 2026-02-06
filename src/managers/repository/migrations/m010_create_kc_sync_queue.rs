use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{big_integer, big_integer_null, big_unsigned, string, unsigned},
    sea_query,
};

#[derive(Iden)]
enum KcSyncQueue {
    Table,
    BlockchainId,
    ContractAddress,
    KcId,
    RetryCount,
    CreatedAt,
    LastRetryAt,
}

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(KcSyncQueue::Table)
                    .if_not_exists()
                    .col(string(KcSyncQueue::BlockchainId))
                    .col(string(KcSyncQueue::ContractAddress))
                    .col(big_unsigned(KcSyncQueue::KcId))
                    .col(unsigned(KcSyncQueue::RetryCount).default(0))
                    .col(big_integer(KcSyncQueue::CreatedAt))
                    .col(big_integer_null(KcSyncQueue::LastRetryAt))
                    .primary_key(
                        Index::create()
                            .col(KcSyncQueue::BlockchainId)
                            .col(KcSyncQueue::ContractAddress)
                            .col(KcSyncQueue::KcId),
                    )
                    .to_owned(),
            )
            .await?;

        // Index for efficient querying of pending items per contract
        manager
            .create_index(
                Index::create()
                    .name("idx_kc_sync_queue_pending")
                    .table(KcSyncQueue::Table)
                    .col(KcSyncQueue::BlockchainId)
                    .col(KcSyncQueue::ContractAddress)
                    .col(KcSyncQueue::RetryCount)
                    .col(KcSyncQueue::KcId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(KcSyncQueue::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
