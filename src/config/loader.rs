use std::{path::Path, sync::OnceLock};

use clap::{Arg, Command};
use figment::{
    Figment,
    providers::{Format, Serialized, Toml},
};
use serde::Deserialize;

use super::{Config, ConfigRaw, defaults};
use crate::config::ConfigError;

static CONFIG_ENV: OnceLock<String> = OnceLock::new();

#[derive(Debug, Deserialize)]
struct EnvironmentConfig {
    environment: Option<String>,
}

/// Returns the currently selected environment.
/// This is set during configuration initialization.
pub(crate) fn current_env() -> &'static str {
    CONFIG_ENV
        .get()
        .map(std::string::String::as_str)
        .expect("configuration environment not initialized")
}

/// Returns true if running in a development environment.
/// Derived from config/environment (true if "development").
pub(crate) fn is_dev_env() -> bool {
    matches!(current_env(), "development")
}

pub(crate) fn initialize_configuration() -> Config {
    load_configuration().unwrap_or_else(|error| {
        tracing::error!(error = %error, "Failed to load configuration");
        panic!("Failed to load configuration: {error}");
    })
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

    tracing::info!(environment = %node_env, "Loading configuration");

    // Build configuration with layered sources (priority: lowest to highest)
    let mut figment = Figment::from(Serialized::defaults(defaults::config_for(&node_env)?));

    // User overrides from config.toml
    if Path::new("config.toml").exists() {
        figment = figment.merge(Toml::file("config.toml"));
    }

    // If custom config file is provided, merge it with highest priority
    if let Some(config_path) = custom_config_path {
        tracing::info!(path = %config_path, "Loading custom config file");
        figment = figment.merge(Toml::file(config_path));
    }

    // Extract configuration from files
    let config: ConfigRaw = figment.extract().map_err(ConfigError::from)?;
    if config.environment != node_env {
        return Err(ConfigError::UnknownEnvironment(format!(
            "config environment '{}' does not match selected '{}'",
            config.environment, node_env
        )));
    }

    tracing::info!(environment = %node_env, "Configuration loaded successfully");

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

    let env = read_environment_from(config_path)?.ok_or_else(|| {
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

fn read_environment_from(path: &str) -> Result<Option<String>, ConfigError> {
    if !Path::new(path).exists() {
        return Ok(None);
    }

    let config: EnvironmentConfig = Figment::from(Toml::file(path))
        .extract()
        .map_err(ConfigError::from)?;
    Ok(config.environment.map(normalize_env))
}

fn normalize_env(env: String) -> String {
    env.trim().to_lowercase()
}
