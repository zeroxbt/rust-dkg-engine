use std::{env, path::PathBuf};

use clap::{Arg, Command};
use figment::{
    Figment,
    providers::{Format, Toml},
};
use serde::Deserialize;
use thiserror::Error;

use crate::{
    controllers::http_api_controller::http_api_router::HttpApiConfig,
    managers::{
        blockchain::BlockchainManagerConfig, network::NetworkManagerConfig,
        repository::RepositoryManagerConfig, triple_store::TripleStoreManagerConfig,
    },
};

#[derive(Error, Debug)]
pub(crate) enum ConfigError {
    #[error("Configuration loading failed: {0}")]
    LoadError(#[from] Box<figment::Error>),

    #[error("Missing required secret: {0}")]
    MissingSecret(String),
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

/// Returns true if running in a development environment.
/// Derived from NODE_ENV environment variable (true if "development" or "devnet").
pub(crate) fn is_dev_env() -> bool {
    env::var("NODE_ENV")
        .map(|v| matches!(v.as_str(), "development" | "devnet"))
        .unwrap_or(true) // default to dev if NODE_ENV not set
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct Config {
    #[serde(default = "default_app_data_path")]
    pub app_data_path: PathBuf,
    pub managers: ManagersConfig,
    pub http_api: HttpApiConfig,
    #[serde(default)]
    pub cleanup: CleanupConfig,
    #[serde(default)]
    pub logger: LoggerConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

fn default_app_data_path() -> PathBuf {
    PathBuf::from("data".to_string())
}

/// Logger configuration for tracing output.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LoggerConfig {
    /// Log level filter (e.g., "info", "debug", "trace", or module-specific like
    /// "rust_ot_node=debug,network=trace")
    #[serde(default = "default_log_level")]
    pub level: String,
    /// Output format: "pretty" for human-readable, "json" for structured JSON logs
    #[serde(default = "default_log_format")]
    pub format: LogFormat,
}

impl Default for LoggerConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogFormat {
    #[default]
    Pretty,
    Json,
}

fn default_log_level() -> String {
    "rust_ot_node=info".to_string()
}

fn default_log_format() -> LogFormat {
    LogFormat::Pretty
}

/// OpenTelemetry configuration for distributed tracing.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct TelemetryConfig {
    /// Whether to enable OpenTelemetry tracing export
    #[serde(default)]
    pub enabled: bool,
    /// OTLP endpoint for trace export (e.g., "http://tempo:4317")
    #[serde(default = "default_otlp_endpoint")]
    pub otlp_endpoint: String,
    /// Service name reported in traces
    #[serde(default = "default_service_name")]
    pub service_name: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            otlp_endpoint: default_otlp_endpoint(),
            service_name: default_service_name(),
        }
    }
}

fn default_otlp_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_service_name() -> String {
    "rust-ot-node".to_string()
}

/// Cleanup configuration for periodic maintenance tasks.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct CleanupConfig {
    #[serde(default = "default_cleanup_enabled")]
    pub enabled: bool,
    #[serde(default = "default_cleanup_interval_secs")]
    pub interval_secs: u64,
    #[serde(default)]
    pub operations: OperationsCleanupConfig,
    #[serde(default)]
    pub pending_storage: PendingStorageCleanupConfig,
    #[serde(default)]
    pub finality_acks: FinalityAcksCleanupConfig,
    #[serde(default)]
    pub proof_challenges: ProofChallengesCleanupConfig,
    #[serde(default)]
    pub kc_sync_queue: KcSyncQueueCleanupConfig,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            enabled: default_cleanup_enabled(),
            interval_secs: default_cleanup_interval_secs(),
            operations: OperationsCleanupConfig::default(),
            pending_storage: PendingStorageCleanupConfig::default(),
            finality_acks: FinalityAcksCleanupConfig::default(),
            proof_challenges: ProofChallengesCleanupConfig::default(),
            kc_sync_queue: KcSyncQueueCleanupConfig::default(),
        }
    }
}

fn default_cleanup_enabled() -> bool {
    true
}

fn default_cleanup_interval_secs() -> u64 {
    60 * 60
}

/// Cleanup config for operation status + result cache.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct OperationsCleanupConfig {
    #[serde(default = "default_operations_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_repo_batch_size")]
    pub batch_size: usize,
}

impl Default for OperationsCleanupConfig {
    fn default() -> Self {
        Self {
            ttl_secs: default_operations_ttl_secs(),
            batch_size: default_repo_batch_size(),
        }
    }
}

fn default_operations_ttl_secs() -> u64 {
    24 * 60 * 60
}

/// Cleanup config for pending storage (redb).
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct PendingStorageCleanupConfig {
    #[serde(default = "default_pending_storage_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_kv_batch_size")]
    pub batch_size: usize,
}

impl Default for PendingStorageCleanupConfig {
    fn default() -> Self {
        Self {
            ttl_secs: default_pending_storage_ttl_secs(),
            batch_size: default_kv_batch_size(),
        }
    }
}

fn default_pending_storage_ttl_secs() -> u64 {
    12 * 60 * 60
}

/// Cleanup config for finality ack records.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct FinalityAcksCleanupConfig {
    #[serde(default = "default_finality_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_repo_batch_size")]
    pub batch_size: usize,
}

impl Default for FinalityAcksCleanupConfig {
    fn default() -> Self {
        Self {
            ttl_secs: default_finality_ttl_secs(),
            batch_size: default_repo_batch_size(),
        }
    }
}

fn default_finality_ttl_secs() -> u64 {
    24 * 60 * 60
}

/// Cleanup config for proof challenge records.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct ProofChallengesCleanupConfig {
    #[serde(default = "default_proof_challenges_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_compound_batch_size")]
    pub batch_size: usize,
}

impl Default for ProofChallengesCleanupConfig {
    fn default() -> Self {
        Self {
            ttl_secs: default_proof_challenges_ttl_secs(),
            batch_size: default_compound_batch_size(),
        }
    }
}

fn default_proof_challenges_ttl_secs() -> u64 {
    7 * 24 * 60 * 60
}

/// Cleanup config for KC sync queue.
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct KcSyncQueueCleanupConfig {
    #[serde(default = "default_kc_sync_queue_ttl_secs")]
    pub ttl_secs: u64,
    #[serde(default = "default_kc_sync_queue_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_compound_batch_size")]
    pub batch_size: usize,
}

impl Default for KcSyncQueueCleanupConfig {
    fn default() -> Self {
        Self {
            ttl_secs: default_kc_sync_queue_ttl_secs(),
            max_retries: default_kc_sync_queue_max_retries(),
            batch_size: default_compound_batch_size(),
        }
    }
}

fn default_kc_sync_queue_ttl_secs() -> u64 {
    24 * 60 * 60
}

fn default_kc_sync_queue_max_retries() -> u32 {
    2
}

fn default_repo_batch_size() -> usize {
    50_000
}

fn default_kv_batch_size() -> usize {
    50_000
}

fn default_compound_batch_size() -> usize {
    1_000
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct ManagersConfig {
    pub network: NetworkManagerConfig,
    pub repository: RepositoryManagerConfig,
    pub blockchain: BlockchainManagerConfig,
    pub triple_store: TripleStoreManagerConfig,
}

pub(crate) fn initialize_configuration() -> Config {
    load_configuration().expect("Failed to load configuration")
}

fn load_configuration() -> Result<Config, ConfigError> {
    let node_env = env::var("NODE_ENV").unwrap_or_else(|_| "development".to_string());

    tracing::info!("Loading configuration for environment: {}", node_env);

    // Build configuration with layered sources (priority: lowest to highest)
    let mut figment = Figment::new()
        // Base configuration from TOML
        .merge(Toml::file(format!("config/{}.toml", node_env)))
        // User overrides from config.toml
        .merge(Toml::file("config.toml"));

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

    // If custom config file is provided, merge it with highest priority
    if let Some(config_path) = matches.get_one::<String>("config") {
        tracing::info!("Loading custom config file: {}", config_path);
        figment = figment.merge(Toml::file(config_path));
    }

    // Extract configuration from files
    let mut config: Config = figment.extract().map_err(Box::new)?;

    // Resolve secrets from environment variables (env vars take precedence over config files)
    resolve_secrets(&mut config)?;

    tracing::info!("Configuration loaded successfully");

    Ok(config)
}

/// Resolves secrets from environment variables into the config.
/// Environment variables take precedence over config file values.
///
/// Required secrets:
/// - `EVM_OPERATIONAL_WALLET_PRIVATE_KEY` - operational wallet private key
/// - `DB_PASSWORD` - database password
///
/// Optional secrets:
/// - `EVM_MANAGEMENT_WALLET_PRIVATE_KEY` - management wallet private key
fn resolve_secrets(config: &mut Config) -> Result<(), ConfigError> {
    // Resolve database password
    if let Ok(password) = env::var("DB_PASSWORD") {
        config.managers.repository.set_password(password);
    }
    config.managers.repository.ensure_password()?;

    // Resolve blockchain secrets for each configured blockchain
    let operational_key = env::var("EVM_OPERATIONAL_WALLET_PRIVATE_KEY").ok();
    let management_key = env::var("EVM_MANAGEMENT_WALLET_PRIVATE_KEY").ok();

    for blockchain_config in config.managers.blockchain.configs_mut() {
        // Set operational wallet private key from env if available
        if let Some(ref key) = operational_key {
            blockchain_config.set_operational_wallet_private_key(key.clone());
        }
        blockchain_config.ensure_operational_wallet_private_key()?;

        // Set management wallet private key from env if available (optional)
        if let Some(ref key) = management_key {
            blockchain_config.set_management_wallet_private_key(key.clone());
        }
    }

    Ok(())
}
