mod migrations;
pub mod models;
mod repositories;

use repositories::command_repository::CommandRepository;
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};
use sea_orm_migration::MigratorTrait;
use serde::Deserialize;
use std::{error::Error, sync::Arc};

use self::migrations::Migrator;

pub struct RepositoryManager {
    config: RepositoryConfig,
    connection: Arc<DatabaseConnection>,
    command_repository: CommandRepository,
}

impl RepositoryManager {
    pub async fn new(config: RepositoryConfig) -> Result<Self, Box<dyn Error>> {
        // Connect to MySQL server
        let conn = Database::connect(config.root_connection_string()).await?;

        // Create the database if it doesn't exist
        conn.execute(Statement::from_string(
            DbBackend::MySql,
            format!("CREATE DATABASE IF NOT EXISTS {}", config.database),
        ))
        .await?;

        // Establish connection to the specific database
        let conn = Arc::new(Database::connect(&config.connection_string()).await?);

        // Apply all pending migrations
        Migrator::up(conn.as_ref(), None).await?;

        Ok(RepositoryManager {
            config,
            connection: Arc::clone(&conn),
            command_repository: CommandRepository::new(Arc::clone(&conn)),
        })
    }

    pub fn get_command_repository(&self) -> &CommandRepository {
        &self.command_repository
    }
}

#[derive(Debug, Deserialize)]
pub struct RepositoryConfig {
    user: String,
    password: String,
    database: String,
    host: String,
    port: u16,
}

impl RepositoryConfig {
    fn new(user: String, password: String, database: String, host: String, port: u16) -> Self {
        Self {
            user,
            password,
            database,
            host,
            port,
        }
    }

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
