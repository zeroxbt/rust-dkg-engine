pub(crate) mod error;
mod migrations;
mod models;
mod repositories;
mod types;

use std::sync::Arc;

use error::RepositoryError;
pub(crate) use repositories::shard_repository::ShardRecordInput;
use repositories::{
    blockchain_repository::BlockchainRepository,
    finality_status_repository::FinalityStatusRepository,
    operation_repository::OperationRepository, shard_repository::ShardRepository,
    triples_insert_count_repository::TriplesInsertCountRepository,
};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use sea_orm_migration::MigratorTrait;
use serde::Deserialize;
pub(crate) use types::OperationStatus;

use self::migrations::Migrator;

pub(crate) struct RepositoryManager {
    shard_repository: ShardRepository,
    blockchain_repository: BlockchainRepository,
    operation_repository: OperationRepository,
    finality_status_repository: FinalityStatusRepository,
    triples_insert_count_repository: TriplesInsertCountRepository,
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
            shard_repository: ShardRepository::new(Arc::clone(&conn)),
            blockchain_repository: BlockchainRepository::new(Arc::clone(&conn)),
            operation_repository: OperationRepository::new(Arc::clone(&conn)),
            finality_status_repository: FinalityStatusRepository::new(Arc::clone(&conn)),
            triples_insert_count_repository: TriplesInsertCountRepository::new(Arc::clone(&conn)),
        })
    }

    pub(crate) fn shard_repository(&self) -> &ShardRepository {
        &self.shard_repository
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
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct RepositoryManagerConfig {
    user: String,
    password: String,
    database: String,
    host: String,
    port: u16,
    max_connections: u32,
    min_connections: u32,
}

impl RepositoryManagerConfig {
    fn root_connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}",
            self.user, self.password, self.host, self.port
        )
    }

    fn connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}
