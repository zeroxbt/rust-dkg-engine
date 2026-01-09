pub mod error;
mod migrations;
pub mod models;
mod repositories;

use error::RepositoryError;
use repositories::{
    blockchain_repository::BlockchainRepository, command_repository::CommandRepository,
    operation_repository::OperationRepository, shard_repository::ShardRepository,
};
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DbBackend, Statement};
use sea_orm_migration::MigratorTrait;
use serde::Deserialize;
use std::sync::Arc;

use self::migrations::Migrator;

pub struct RepositoryManager {
    command_repository: CommandRepository,
    shard_repository: ShardRepository,
    blockchain_repository: BlockchainRepository,
    operation_repository: OperationRepository,
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
    pub async fn new(config: &RepositoryManagerConfig) -> Result<Self, RepositoryError> {
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
            command_repository: CommandRepository::new(Arc::clone(&conn)),
            shard_repository: ShardRepository::new(Arc::clone(&conn)),
            blockchain_repository: BlockchainRepository::new(Arc::clone(&conn)),
            operation_repository: OperationRepository::new(Arc::clone(&conn)),
        })
    }

    pub fn command_repository(&self) -> &CommandRepository {
        &self.command_repository
    }

    pub fn shard_repository(&self) -> &ShardRepository {
        &self.shard_repository
    }

    pub fn blockchain_repository(&self) -> &BlockchainRepository {
        &self.blockchain_repository
    }

    pub fn operation_repository(&self) -> &OperationRepository {
        &self.operation_repository
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct RepositoryManagerConfig {
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
