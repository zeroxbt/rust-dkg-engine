use serde::{Deserialize, Serialize};

use crate::config::ConfigError;

/// Repository manager configuration for database connections.
///
/// **Secret handling**: Database password should be provided via configuration
/// (resolved at config load time) or environment variable:
/// - `DB_PASSWORD` - database password (required)
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct RepositoryManagerConfigRaw {
    pub(crate) user: String,
    pub(crate) password: Option<String>,
    pub(crate) database: String,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) max_connections: u32,
    pub(crate) min_connections: u32,
}

impl RepositoryManagerConfigRaw {
    pub(crate) fn resolve(self) -> Result<RepositoryManagerConfig, ConfigError> {
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
pub(crate) struct RepositoryManagerConfig {
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) database: String,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) max_connections: u32,
    pub(crate) min_connections: u32,
}

impl RepositoryManagerConfig {
    pub(crate) fn root_connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}",
            self.user, self.password, self.host, self.port
        )
    }

    pub(crate) fn connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}
