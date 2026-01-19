use serde::Deserialize;

/// Configuration for the Triple Store Manager
#[derive(Debug, Deserialize, Clone)]
pub struct TripleStoreManagerConfig {
    /// Base URL of the triple store service (e.g., "http://localhost:9999")
    pub url: String,

    /// Repository/namespace name (default: "DKG")
    #[serde(default = "default_repository")]
    pub repository: String,

    /// Optional username for authentication
    pub username: Option<String>,

    /// Optional password for authentication
    pub password: Option<String>,

    /// Maximum number of connection retries on startup
    #[serde(default = "default_connect_max_retries")]
    pub connect_max_retries: u32,

    /// Delay between connection retry attempts in milliseconds
    #[serde(default = "default_connect_retry_frequency_ms")]
    pub connect_retry_frequency_ms: u64,

    /// Timeout configuration for different operation types
    #[serde(default)]
    pub timeouts: TimeoutConfig,
}

/// Timeout configuration for different SPARQL operations
#[derive(Debug, Deserialize, Clone)]
pub struct TimeoutConfig {
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

fn default_repository() -> String {
    "DKG".to_string()
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

impl TripleStoreManagerConfig {
    /// Get the SPARQL endpoint URL for the configured repository
    pub fn sparql_endpoint(&self) -> String {
        format!(
            "{}/blazegraph/namespace/{}/sparql",
            self.url.trim_end_matches('/'),
            self.repository
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
