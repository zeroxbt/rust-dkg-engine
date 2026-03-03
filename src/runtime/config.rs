use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeConfig {
    pub graceful_shutdown: GracefulShutdownConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GracefulShutdownConfig {
    pub periodic_tasks_timeout_secs: u64,
    pub command_executor_timeout_secs: u64,
    pub network_event_loop_timeout_secs: u64,
    pub peer_registry_timeout_secs: u64,
    pub http_server_timeout_secs: u64,
}
