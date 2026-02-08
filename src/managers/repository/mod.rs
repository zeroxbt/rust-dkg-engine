mod config;
pub(crate) mod error;
mod migrations;
mod models;
mod repositories;
mod types;

use std::sync::Arc;

pub(crate) use config::RepositoryManagerConfig;
use error::RepositoryError;
pub(crate) use repositories::proof_challenge_repository::ChallengeState;
use repositories::{
    blockchain_repository::BlockchainRepository,
    finality_status_repository::FinalityStatusRepository, kc_sync_repository::KcSyncRepository,
    operation_repository::OperationRepository,
    proof_challenge_repository::ProofChallengeRepository,
    triples_insert_count_repository::TriplesInsertCountRepository,
};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use sea_orm_migration::MigratorTrait;
pub(crate) use types::OperationStatus;

use self::migrations::Migrator;

pub(crate) struct RepositoryManager {
    blockchain_repository: BlockchainRepository,
    operation_repository: OperationRepository,
    finality_status_repository: FinalityStatusRepository,
    triples_insert_count_repository: TriplesInsertCountRepository,
    kc_sync_repository: KcSyncRepository,
    proof_challenge_repository: ProofChallengeRepository,
}

impl RepositoryManager {
    /// Creates a new RepositoryManager instance
    ///
    /// # Errors
    ///
    /// Returns `RepositoryError` if:
    /// - Database connection fails
    /// - Database creation fails
    /// - Migrations fail
    pub(crate) async fn connect(config: &RepositoryManagerConfig) -> Result<Self, RepositoryError> {
        // Connect to MySQL server
        let conn = Database::connect(config.root_connection_string()).await?;

        // Create the database if it doesn't exist
        conn.execute(Statement::from_string(
            DbBackend::MySql,
            format!("CREATE DATABASE IF NOT EXISTS {}", config.database),
        ))
        .await?;

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
            proof_challenge_repository: ProofChallengeRepository::new(Arc::clone(&conn)),
        })
    }

    pub(crate) fn blockchain_repository(&self) -> &BlockchainRepository {
        &self.blockchain_repository
    }

    pub(crate) fn operation_repository(&self) -> &OperationRepository {
        &self.operation_repository
    }

    pub(crate) fn finality_status_repository(&self) -> &FinalityStatusRepository {
        &self.finality_status_repository
    }

    pub(crate) fn triples_insert_count_repository(&self) -> &TriplesInsertCountRepository {
        &self.triples_insert_count_repository
    }

    pub(crate) fn kc_sync_repository(&self) -> &KcSyncRepository {
        &self.kc_sync_repository
    }

    pub(crate) fn proof_challenge_repository(&self) -> &ProofChallengeRepository {
        &self.proof_challenge_repository
    }
}
