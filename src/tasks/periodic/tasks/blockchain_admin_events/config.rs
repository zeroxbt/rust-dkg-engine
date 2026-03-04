use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockchainAdminEventsConfig {
    /// Poll interval for admin event fetch/processing loop.
    pub poll_interval_secs: u64,
}
