use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    config::ConfigError,
    controllers::ControllersConfig,
    logger::{LoggerConfig, TelemetryConfig},
    managers::{ManagersConfig, ManagersConfigRaw},
    periodic_tasks::PeriodicTasksConfig,
};

/// Centralized application paths derived from the root data directory.
///
/// This provides a single source of truth for all filesystem paths used by the application,
/// making it easy to see the complete directory structure and avoiding path collisions.
#[derive(Debug, Clone)]
pub(crate) struct AppPaths {
    /// Path to the network identity key file
    pub network_key: PathBuf,
    /// Path to the key-value store database
    pub key_value_store: PathBuf,
    /// Path to the triple store directory
    pub triple_store: PathBuf,
}

impl AppPaths {
    /// Create AppPaths from a root data directory.
    ///
    /// Directory structure:
    /// ```text
    /// {root}/
    /// ├── network/
    /// │   └── private_key          <- network identity key
    /// ├── key-value-store/
    /// │   └── key_value_store.redb <- operation tracking database
    /// └── triple-store/
    ///     └── {repository}/        <- oxigraph store per repository
    /// ```
    pub(crate) fn from_root(root: PathBuf) -> Self {
        Self {
            network_key: root.join("network/private_key"),
            key_value_store: root.join("key-value-store/key_value_store.redb"),
            triple_store: root.join("triple-store"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigRaw {
    pub environment: String,
    pub app_data_path: PathBuf,
    pub managers: ManagersConfigRaw,
    pub controllers: ControllersConfig,
    pub periodic_tasks: PeriodicTasksConfig,
    pub logger: LoggerConfig,
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub app_data_path: PathBuf,
    pub managers: ManagersConfig,
    pub controllers: ControllersConfig,
    pub periodic_tasks: PeriodicTasksConfig,
    pub logger: LoggerConfig,
    pub telemetry: TelemetryConfig,
}

impl ConfigRaw {
    pub(crate) fn resolve(self) -> Result<Config, ConfigError> {
        Ok(Config {
            app_data_path: self.app_data_path,
            managers: self.managers.resolve()?,
            controllers: self.controllers,
            periodic_tasks: self.periodic_tasks,
            logger: self.logger,
            telemetry: self.telemetry,
        })
    }
}
