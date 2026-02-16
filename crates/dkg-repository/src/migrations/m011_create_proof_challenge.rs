use sea_orm_migration::{
    async_trait::async_trait,
    prelude::{DbErr, DeriveMigrationName, Iden, Index, MigrationTrait, SchemaManager, Table},
    schema::{big_integer, string, string_null},
    sea_query,
};

#[derive(Iden)]
enum ProofChallenge {
    Table,
    BlockchainId,
    Epoch,
    ProofPeriodStartBlock,
    ContractAddress,
    KnowledgeCollectionId,
    ChunkIndex,
    State,
    Score,
    CreatedAt,
    UpdatedAt,
}

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(ProofChallenge::Table)
                    .if_not_exists()
                    .col(string(ProofChallenge::BlockchainId))
                    .col(big_integer(ProofChallenge::Epoch))
                    .col(big_integer(ProofChallenge::ProofPeriodStartBlock))
                    .col(string(ProofChallenge::ContractAddress))
                    .col(big_integer(ProofChallenge::KnowledgeCollectionId))
                    .col(big_integer(ProofChallenge::ChunkIndex))
                    .col(string(ProofChallenge::State).default("pending"))
                    .col(string_null(ProofChallenge::Score))
                    .col(big_integer(ProofChallenge::CreatedAt))
                    .col(big_integer(ProofChallenge::UpdatedAt))
                    .primary_key(
                        Index::create()
                            .col(ProofChallenge::BlockchainId)
                            .col(ProofChallenge::Epoch)
                            .col(ProofChallenge::ProofPeriodStartBlock),
                    )
                    .to_owned(),
            )
            .await?;

        // Index for efficient querying by blockchain
        manager
            .create_index(
                Index::create()
                    .name("idx_proof_challenge_blockchain")
                    .table(ProofChallenge::Table)
                    .col(ProofChallenge::BlockchainId)
                    .col(ProofChallenge::CreatedAt)
                    .to_owned(),
            )
            .await?;

        // Index for cleanup queries by updated_at
        manager
            .create_index(
                Index::create()
                    .name("idx_proof_challenge_updated_at")
                    .table(ProofChallenge::Table)
                    .col(ProofChallenge::UpdatedAt)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(
                Table::drop()
                    .table(ProofChallenge::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await
    }
}
