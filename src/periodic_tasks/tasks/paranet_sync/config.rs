use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ParanetSyncConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub batch_size: usize,
    pub max_in_flight: usize,
    pub retries_limit: u32,
    pub retry_delay_secs: u64,
    pub sync_paranets: Vec<String>,
}
