use serde::Deserialize;

use crate::config::ConfigError;

/// Repository manager configuration for database connections.
///
/// **Secret handling**: Database password should be provided via configuration
/// (resolved at config load time) or environment variable:
/// - `DB_PASSWORD` - database password (required)
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct RepositoryManagerConfig {
    pub(crate) user: String,
    pub(crate) password: Option<String>,
    pub(crate) database: String,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) max_connections: u32,
    pub(crate) min_connections: u32,
}

impl RepositoryManagerConfig {
    /// Returns the database password.
    /// This is guaranteed to be set after config initialization.
    fn password(&self) -> &str {
        self.password
            .as_ref()
            .expect("password should be set during config initialization")
    }

    /// Sets the database password (called during secret resolution).
    pub(crate) fn set_password(&mut self, password: String) {
        self.password = Some(password);
    }

    /// Ensures the database password is set.
    pub(crate) fn ensure_password(&self) -> Result<(), ConfigError> {
        if self.password.is_none() {
            return Err(ConfigError::MissingSecret(
                "DB_PASSWORD env var or password config required".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn root_connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}",
            self.user,
            self.password(),
            self.host,
            self.port
        )
    }

    pub(crate) fn connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user,
            self.password(),
            self.host,
            self.port,
            self.database
        )
    }
}
