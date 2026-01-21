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
