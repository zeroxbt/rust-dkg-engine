use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct SyncConfig {
    pub enabled: bool,
    #[serde(default = "default_head_safety_blocks")]
    pub head_safety_blocks: u64,
    #[serde(default = "default_sync_idle_sleep_secs")]
    pub sync_idle_sleep_secs: u64,
    #[serde(default = "default_metadata_backfill_block_batch_size")]
    pub metadata_backfill_block_batch_size: u64,
    #[serde(default = "default_metadata_state_batch_size")]
    pub metadata_state_batch_size: usize,
    #[serde(default = "default_metadata_stage_batch_size")]
    pub metadata_stage_batch_size: usize,
    #[serde(default = "default_metadata_backfill_enabled")]
    pub metadata_backfill_enabled: bool,
    pub period_catching_up_secs: u64,
    pub period_idle_secs: u64,
    pub no_peers_retry_delay_secs: u64,
    pub max_retry_attempts: u32,
    pub max_new_kcs_per_contract: u64,
    pub filter_batch_size: usize,
    pub network_fetch_batch_size: usize,
    pub batch_get_fanout_concurrency: usize,
    pub max_assets_per_fetch_batch: u64,
    pub insert_batch_concurrency: usize,
    pub pipeline_channel_buffer: usize,
    pub retry_base_delay_secs: u64,
    pub retry_max_delay_secs: u64,
    pub retry_jitter_secs: u64,
}

const fn default_head_safety_blocks() -> u64 {
    2
}

const fn default_sync_idle_sleep_secs() -> u64 {
    30
}

const fn default_metadata_backfill_block_batch_size() -> u64 {
    400
}

const fn default_metadata_state_batch_size() -> usize {
    200
}

const fn default_metadata_stage_batch_size() -> usize {
    100
}

const fn default_metadata_backfill_enabled() -> bool {
    true
}
