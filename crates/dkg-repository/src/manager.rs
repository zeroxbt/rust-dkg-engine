use std::sync::Arc;

use sea_orm::{ConnectOptions, Database};
use sea_orm_migration::MigratorTrait;

pub use crate::config::RepositoryManagerConfig;
use crate::{
    error::RepositoryError,
    migrations::Migrator,
    repositories::{
        blockchain_repository::BlockchainRepository,
        finality_status_repository::FinalityStatusRepository, kc_sync_repository::KcSyncRepository,
        operation_repository::OperationRepository,
        paranet_kc_sync_repository::ParanetKcSyncRepository,
        proof_challenge_repository::ProofChallengeRepository,
        triples_insert_count_repository::TriplesInsertCountRepository,
    },
};

pub struct RepositoryManager {
    blockchain_repository: BlockchainRepository,
    operation_repository: OperationRepository,
    finality_status_repository: FinalityStatusRepository,
    triples_insert_count_repository: TriplesInsertCountRepository,
    kc_sync_repository: KcSyncRepository,
    paranet_kc_sync_repository: ParanetKcSyncRepository,
    proof_challenge_repository: ProofChallengeRepository,
}

impl RepositoryManager {
    /// Creates a new RepositoryManager instance
    ///
    /// # Errors
    ///
    /// Returns `RepositoryError` if:
    /// - Database connection fails (e.g. database missing, bad credentials)
    /// - Migrations fail
    pub async fn connect(config: &RepositoryManagerConfig) -> Result<Self, RepositoryError> {
        let mut opt = ConnectOptions::new(config.connection_string());
        opt.max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .sqlx_logging(true)
            .sqlx_logging_level(tracing::log::LevelFilter::Debug);

        // Establish connection to the specific database
        let conn = Arc::new(Database::connect(opt).await?);

        // Apply all pending migrations
        Migrator::up(conn.as_ref(), None).await?;

        Ok(RepositoryManager {
            blockchain_repository: BlockchainRepository::new(Arc::clone(&conn)),
            operation_repository: OperationRepository::new(Arc::clone(&conn)),
            finality_status_repository: FinalityStatusRepository::new(Arc::clone(&conn)),
            triples_insert_count_repository: TriplesInsertCountRepository::new(Arc::clone(&conn)),
            kc_sync_repository: KcSyncRepository::new(Arc::clone(&conn)),
            paranet_kc_sync_repository: ParanetKcSyncRepository::new(Arc::clone(&conn)),
            proof_challenge_repository: ProofChallengeRepository::new(Arc::clone(&conn)),
        })
    }

    pub fn blockchain_repository(&self) -> BlockchainRepository {
        self.blockchain_repository.clone()
    }

    pub fn operation_repository(&self) -> OperationRepository {
        self.operation_repository.clone()
    }

    pub fn finality_status_repository(&self) -> FinalityStatusRepository {
        self.finality_status_repository.clone()
    }

    pub fn triples_insert_count_repository(&self) -> TriplesInsertCountRepository {
        self.triples_insert_count_repository.clone()
    }

    pub fn kc_sync_repository(&self) -> KcSyncRepository {
        self.kc_sync_repository.clone()
    }

    pub fn paranet_kc_sync_repository(&self) -> ParanetKcSyncRepository {
        self.paranet_kc_sync_repository.clone()
    }

    pub fn proof_challenge_repository(&self) -> ProofChallengeRepository {
        self.proof_challenge_repository.clone()
    }
}
