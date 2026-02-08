//! Network manager configuration.

use std::time::Duration;

use serde::Deserialize;

/// Configuration for the network manager.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NetworkManagerConfig {
    pub port: u32,
    pub bootstrap: Vec<String>,
    /// External IP address to announce to peers (for NAT traversal).
    /// If set, the node will advertise this address so peers behind NAT can be reached.
    /// Must be a valid public IPv4 address.
    pub external_ip: Option<String>,
    /// How long to keep idle connections open (in seconds).
    pub idle_connection_timeout_secs: u64,
}

impl NetworkManagerConfig {
    /// Get idle connection timeout as Duration
    pub(crate) fn idle_connection_timeout(&self) -> Duration {
        Duration::from_secs(self.idle_connection_timeout_secs)
    }
}
