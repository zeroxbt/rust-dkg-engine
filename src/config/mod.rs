use std::{env, path::PathBuf};

use blockchain::BlockchainManagerConfig;
use figment::{
    Figment,
    providers::{Format, Toml},
};
use network::NetworkManagerConfig;
use repository::RepositoryManagerConfig;
use serde::Deserialize;
use thiserror::Error;
use triple_store::TripleStoreManagerConfig;

use crate::controllers::http_api_controller::http_api_router::HttpApiConfig;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Configuration loading failed: {0}")]
    LoadError(#[from] figment::Error),

    #[error("Configuration validation failed: {0}")]
    ValidationError(String),
}

/// Centralized application paths derived from the root data directory.
///
/// This provides a single source of truth for all filesystem paths used by the application,
/// making it easy to see the complete directory structure and avoiding path collisions.
#[derive(Debug, Clone)]
pub struct AppPaths {
    /// Root data directory (e.g., "data")
    pub root: PathBuf,
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
    pub fn from_root(root: PathBuf) -> Self {
        Self {
            network_key: root.join("network/private_key"),
            key_value_store: root.join("key-value-store/key_value_store.redb"),
            triple_store: root.join("triple-store"),
            root,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub is_dev_env: bool,
    #[serde(default = "default_app_data_path")]
    pub app_data_path: PathBuf,
    pub managers: ManagersConfig,
    pub http_api: HttpApiConfig,
}

fn default_app_data_path() -> PathBuf {
    PathBuf::from("data".to_string())
}

#[derive(Debug, Deserialize, Clone)]
pub struct ManagersConfig {
    pub network: NetworkManagerConfig,
    pub repository: RepositoryManagerConfig,
    pub blockchain: BlockchainManagerConfig,
    pub triple_store: TripleStoreManagerConfig,
}

pub fn initialize_configuration() -> Config {
    load_configuration().expect("Failed to load configuration")
}

fn load_configuration() -> Result<Config, ConfigError> {
    let node_env = env::var("NODE_ENV").unwrap_or_else(|_| "development".to_string());

    tracing::info!("Loading configuration for environment: {}", node_env);

    // Build configuration with layered sources (priority: lowest to highest)
    let mut figment = Figment::new()
        // Base configuration from TOML
        .merge(Toml::file(format!("config/{}.toml", node_env)))
        // User overrides from .origintrail_noderc.toml
        .merge(Toml::file(".origintrail_noderc.toml").nested());

    // Parse CLI arguments for custom config file
    let matches = clap::Command::new("OriginTrail Rust Node")
        .arg(
            clap::Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file (.toml format)"),
        )
        .get_matches();

    if let Some(config_path) = matches.get_one::<String>("config") {
        tracing::info!("Loading custom config file: {}", config_path);
        figment = figment.merge(Toml::file(config_path));
    }

    // Extract and validate configuration
    let config: Config = figment.extract()?;

    tracing::info!("Configuration loaded successfully");

    Ok(config)
}
