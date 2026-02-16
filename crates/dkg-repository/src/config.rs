use serde::{Deserialize, Serialize};

use crate::ConfigError;

/// Repository manager configuration for database connections.
///
/// **Secret handling**: Database password should be provided via configuration
/// (resolved at config load time) or environment variable:
/// - `DB_PASSWORD` - database password (required)
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct RepositoryManagerConfigRaw {
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub host: String,
    pub port: u16,
    pub max_connections: u32,
    pub min_connections: u32,
}

impl RepositoryManagerConfigRaw {
    pub fn resolve(self) -> Result<RepositoryManagerConfig, ConfigError> {
        let password = self.password.ok_or_else(|| {
            ConfigError::MissingSecret(
                "DB_PASSWORD env var or password config required".to_string(),
            )
        })?;

        Ok(RepositoryManagerConfig {
            user: self.user,
            password,
            database: self.database,
            host: self.host,
            port: self.port,
            max_connections: self.max_connections,
            min_connections: self.min_connections,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RepositoryManagerConfig {
    pub user: String,
    pub password: String,
    pub database: String,
    pub host: String,
    pub port: u16,
    pub max_connections: u32,
    pub min_connections: u32,
}

impl RepositoryManagerConfig {
    pub fn connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}
