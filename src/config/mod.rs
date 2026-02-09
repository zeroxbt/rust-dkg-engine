use std::{
    path::{Path, PathBuf},
    sync::OnceLock,
};

use clap::{Arg, Command};
pub(crate) mod defaults;

use figment::{
    Figment,
    providers::{Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    commands::periodic::cleanup::CleanupConfig,
    controllers::{http_api_controller::router::HttpApiConfig, rpc_controller::RpcConfig},
    logger::{LoggerConfig, TelemetryConfig},
    managers::{ManagersConfig, ManagersConfigRaw},
};

#[derive(Error, Debug)]
pub(crate) enum ConfigError {
    #[error("Configuration loading failed: {0}")]
    LoadError(#[from] Box<figment::Error>),

    #[error("Missing required secret: {0}")]
    MissingSecret(String),

    #[error("Missing required config file: {0}")]
    MissingConfig(String),

    #[error("Missing required environment setting: {0}")]
    MissingEnvironment(String),

    #[error("Unknown environment: {0}")]
    UnknownEnvironment(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

static CONFIG_ENV: OnceLock<String> = OnceLock::new();

#[derive(Debug, Deserialize)]
struct EnvironmentConfig {
    environment: Option<String>,
}

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

/// Returns the currently selected environment.
/// This is set during configuration initialization.
pub(crate) fn current_env() -> String {
    CONFIG_ENV
        .get()
        .cloned()
        .expect("configuration environment not initialized")
}

/// Returns true if running in a development environment.
/// Derived from config/environment (true if "development").
pub(crate) fn is_dev_env() -> bool {
    matches!(current_env().as_str(), "development")
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigRaw {
    pub environment: String,
    pub app_data_path: PathBuf,
    pub managers: ManagersConfigRaw,
    pub http_api: HttpApiConfig,
    pub rpc: RpcConfig,
    pub cleanup: CleanupConfig,
    pub logger: LoggerConfig,
    pub telemetry: TelemetryConfig,
}

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub app_data_path: PathBuf,
    pub managers: ManagersConfig,
    pub http_api: HttpApiConfig,
    pub rpc: RpcConfig,
    pub cleanup: CleanupConfig,
    pub logger: LoggerConfig,
    pub telemetry: TelemetryConfig,
}

impl ConfigRaw {
    pub(crate) fn resolve(self) -> Result<Config, ConfigError> {
        Ok(Config {
            app_data_path: self.app_data_path,
            managers: self.managers.resolve()?,
            http_api: self.http_api,
            rpc: self.rpc,
            cleanup: self.cleanup,
            logger: self.logger,
            telemetry: self.telemetry,
        })
    }
}

pub(crate) fn initialize_configuration() -> Config {
    load_configuration().expect("Failed to load configuration")
}

fn load_configuration() -> Result<Config, ConfigError> {
    // Parse CLI arguments for custom config file
    let matches = Command::new("OriginTrail Rust Node")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file (.toml format)"),
        )
        .get_matches();

    let custom_config_path = matches.get_one::<String>("config").map(String::as_str);
    let node_env = resolve_environment(custom_config_path)?;
    set_config_env(&node_env);

    tracing::info!("Loading configuration for environment: {}", node_env);

    // Build configuration with layered sources (priority: lowest to highest)
    let mut figment = Figment::from(Serialized::defaults(defaults::config_for(&node_env)));

    // User overrides from config.toml
    if Path::new("config.toml").exists() {
        figment = figment.merge(Toml::file("config.toml"));
    }

    // If custom config file is provided, merge it with highest priority
    if let Some(config_path) = custom_config_path {
        tracing::info!("Loading custom config file: {}", config_path);
        figment = figment.merge(Toml::file(config_path));
    }

    // Extract configuration from files
    let config: ConfigRaw = figment.extract().map_err(Box::new)?;
    if config.environment != node_env {
        return Err(ConfigError::UnknownEnvironment(format!(
            "config environment '{}' does not match selected '{}'",
            config.environment, node_env
        )));
    }

    tracing::info!("Configuration loaded successfully");

    config.resolve()
}

fn set_config_env(env: &str) {
    let _ = CONFIG_ENV.set(env.to_string());
}

fn resolve_environment(custom_config_path: Option<&str>) -> Result<String, ConfigError> {
    let config_path = custom_config_path.unwrap_or("config.toml");

    if !Path::new(config_path).exists() {
        return Err(ConfigError::MissingConfig(config_path.to_string()));
    }

    let env = read_environment_from(config_path).ok_or_else(|| {
        ConfigError::MissingEnvironment(
            "set environment = \"development|testnet|mainnet\" in your config".to_string(),
        )
    })?;

    let env = normalize_env(env);

    if !matches!(env.as_str(), "development" | "testnet" | "mainnet") {
        return Err(ConfigError::UnknownEnvironment(env));
    }

    Ok(env)
}

fn read_environment_from(path: &str) -> Option<String> {
    if !Path::new(path).exists() {
        return None;
    }

    Figment::from(Toml::file(path))
        .extract::<EnvironmentConfig>()
        .ok()
        .and_then(|config| config.environment)
        .map(normalize_env)
}

fn normalize_env(env: String) -> String {
    env.trim().to_lowercase()
}
