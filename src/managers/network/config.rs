//! Network manager configuration.

use std::time::Duration;

use serde::Deserialize;

use crate::services::PeerRateLimiterConfig;

/// Configuration for the network manager.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct NetworkManagerConfig {
    pub port: u32,
    pub bootstrap: Vec<String>,
    /// External IP address to announce to peers (for NAT traversal).
    /// If set, the node will advertise this address so peers behind NAT can be reached.
    /// Must be a valid public IPv4 address.
    #[serde(default)]
    pub external_ip: Option<String>,
    /// How long to keep idle connections open (in seconds).
    /// Default is 300 (5 minutes).
    #[serde(default = "default_idle_connection_timeout")]
    pub idle_connection_timeout_secs: u64,
    /// Per-peer rate limiting configuration for inbound requests.
    #[serde(default)]
    pub rate_limiter: PeerRateLimiterConfig,
}

fn default_idle_connection_timeout() -> u64 {
    300
}

impl NetworkManagerConfig {
    /// Get idle connection timeout as Duration
    pub(crate) fn idle_connection_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_connection_timeout_secs)
    }
}
