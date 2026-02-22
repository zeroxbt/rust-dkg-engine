use serde::{Deserialize, Serialize};

/// The fixed repository/namespace name used by the DKG.
/// For Blazegraph: namespace name
/// For Oxigraph: subdirectory name under data_path
pub const DKG_REPOSITORY: &str = "DKG";

/// Backend type for the triple store
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TripleStoreBackendType {
    /// Blazegraph HTTP-based backend (requires running Blazegraph server)
    Blazegraph,
    /// Oxigraph embedded Rust-native backend (faster, no external service needed)
    Oxigraph,
}

/// Configuration for the Triple Store Manager
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TripleStoreManagerConfig {
    /// Backend type to use.
    pub backend: TripleStoreBackendType,

    /// Base URL of the triple store service (e.g., "http://localhost:9999")
    /// Required for Blazegraph, ignored for Oxigraph
    pub url: String,

    /// Optional username for authentication (Blazegraph only)
    pub username: Option<String>,

    /// Optional password for authentication (Blazegraph only)
    pub password: Option<String>,

    /// Maximum number of connection retries on startup (Blazegraph only)
    pub connect_max_retries: u32,

    /// Delay between connection retry attempts in milliseconds (Blazegraph only)
    pub connect_retry_frequency_ms: u64,

    /// Timeout configuration for different operation types
    pub timeouts: TimeoutConfig,

    /// Maximum concurrent operations.
    /// Limits how many triple store operations can run simultaneously.
    /// Useful to prevent overwhelming Blazegraph or causing resource contention.
    pub max_concurrent_operations: usize,

    /// Oxigraph-specific storage options.
    ///
    /// Ignored for Blazegraph backend.
    #[serde(default)]
    pub oxigraph: OxigraphStoreConfig,
}

/// Oxigraph storage open options.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct OxigraphStoreConfig {
    /// Cap for RocksDB open files used by Oxigraph.
    ///
    /// If `None`, Oxigraph derives the value from process file descriptor limits.
    pub max_open_files: Option<u32>,

    /// File descriptors reserved for non-RocksDB usage when deriving
    /// `max_open_files` from process limits.
    ///
    /// Ignored when `max_open_files` is explicitly set.
    pub fd_reserve: Option<u32>,
}

/// Timeout configuration for different SPARQL operations
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TimeoutConfig {
    /// Timeout for CONSTRUCT/SELECT queries in milliseconds
    pub query_ms: u64,

    /// Timeout for INSERT/UPDATE operations in milliseconds
    pub insert_ms: u64,

    /// Timeout for ASK queries in milliseconds
    pub ask_ms: u64,
}

impl TimeoutConfig {
    /// Get query timeout as Duration
    pub fn query_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.query_ms)
    }

    /// Get insert timeout as Duration
    pub fn insert_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.insert_ms)
    }

    /// Get ask timeout as Duration
    pub fn ask_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.ask_ms)
    }
}

impl TripleStoreManagerConfig {
    /// Get connect retry frequency as Duration
    pub fn connect_retry_frequency(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.connect_retry_frequency_ms)
    }

    /// Get the SPARQL endpoint URL for the DKG repository
    pub fn sparql_endpoint(&self) -> String {
        format!(
            "{}/blazegraph/namespace/{}/sparql",
            self.url.trim_end_matches('/'),
            DKG_REPOSITORY
        )
    }

    /// Get the namespace management endpoint URL
    pub fn namespace_endpoint(&self) -> String {
        format!("{}/blazegraph/namespace", self.url.trim_end_matches('/'))
    }

    /// Get the status endpoint URL
    pub fn status_endpoint(&self) -> String {
        format!("{}/blazegraph/status", self.url.trim_end_matches('/'))
    }
}
