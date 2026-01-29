use std::path::PathBuf;

use serde::Deserialize;

/// The fixed repository/namespace name used by the DKG.
/// For Blazegraph: namespace name
/// For Oxigraph: subdirectory name under data_path
pub(crate) const DKG_REPOSITORY: &str = "DKG";

/// Backend type for the triple store
#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TripleStoreBackendType {
    /// Blazegraph HTTP-based backend (requires running Blazegraph server)
    Blazegraph,
    /// Oxigraph embedded Rust-native backend (faster, no external service needed)
    #[default]
    Oxigraph,
}

/// Configuration for the Triple Store Manager
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct TripleStoreManagerConfig {
    /// Backend type to use (default: "blazegraph")
    #[serde(default)]
    pub backend: TripleStoreBackendType,

    /// Base URL of the triple store service (e.g., "http://localhost:9999")
    /// Required for Blazegraph, ignored for Oxigraph
    #[serde(default)]
    pub url: String,

    /// Path for Oxigraph persistent storage (default: "{app_data_path}/triple-store")
    /// Only used when backend = "oxigraph"
    pub data_path: Option<PathBuf>,

    /// Optional username for authentication (Blazegraph only)
    pub username: Option<String>,

    /// Optional password for authentication (Blazegraph only)
    pub password: Option<String>,

    /// Maximum number of connection retries on startup (Blazegraph only)
    #[serde(default = "default_connect_max_retries")]
    pub connect_max_retries: u32,

    /// Delay between connection retry attempts in milliseconds (Blazegraph only)
    #[serde(default = "default_connect_retry_frequency_ms")]
    pub connect_retry_frequency_ms: u64,

    /// Timeout configuration for different operation types
    #[serde(default)]
    pub timeouts: TimeoutConfig,

    /// Maximum concurrent operations.
    /// Limits how many triple store operations can run simultaneously.
    /// Useful to prevent overwhelming Blazegraph or causing resource contention.
    #[serde(default = "default_max_concurrent_operations")]
    pub max_concurrent_operations: Option<usize>,
}

/// Timeout configuration for different SPARQL operations
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct TimeoutConfig {
    /// Timeout for CONSTRUCT/SELECT queries in milliseconds
    #[serde(default = "default_query_timeout_ms")]
    pub query_ms: u64,

    /// Timeout for INSERT/UPDATE operations in milliseconds
    #[serde(default = "default_insert_timeout_ms")]
    pub insert_ms: u64,

    /// Timeout for ASK queries in milliseconds
    #[serde(default = "default_ask_timeout_ms")]
    pub ask_ms: u64,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            query_ms: default_query_timeout_ms(),
            insert_ms: default_insert_timeout_ms(),
            ask_ms: default_ask_timeout_ms(),
        }
    }
}

fn default_connect_max_retries() -> u32 {
    10
}

fn default_connect_retry_frequency_ms() -> u64 {
    10_000
}

fn default_query_timeout_ms() -> u64 {
    60_000
}

fn default_insert_timeout_ms() -> u64 {
    300_000
}

fn default_ask_timeout_ms() -> u64 {
    10_000
}

fn default_max_concurrent_operations() -> Option<usize> {
    Some(16)
}

impl TimeoutConfig {
    /// Get query timeout as Duration
    pub(crate) fn query_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.query_ms)
    }

    /// Get insert timeout as Duration
    pub(crate) fn insert_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.insert_ms)
    }

    /// Get ask timeout as Duration
    pub(crate) fn ask_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.ask_ms)
    }
}

impl TripleStoreManagerConfig {
    /// Get connect retry frequency as Duration
    pub(crate) fn connect_retry_frequency(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.connect_retry_frequency_ms)
    }

    /// Get the SPARQL endpoint URL for the DKG repository
    pub(crate) fn sparql_endpoint(&self) -> String {
        format!(
            "{}/blazegraph/namespace/{}/sparql",
            self.url.trim_end_matches('/'),
            DKG_REPOSITORY
        )
    }

    /// Get the namespace management endpoint URL
    pub(crate) fn namespace_endpoint(&self) -> String {
        format!("{}/blazegraph/namespace", self.url.trim_end_matches('/'))
    }

    /// Get the status endpoint URL
    pub(crate) fn status_endpoint(&self) -> String {
        format!("{}/blazegraph/status", self.url.trim_end_matches('/'))
    }
}
