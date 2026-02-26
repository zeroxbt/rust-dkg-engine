use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct SyncConfig {
    pub enabled: bool,
    pub head_safety_blocks: u64,
    pub sync_idle_sleep_secs: u64,
    pub metadata_backfill_block_batch_size: u64,
    pub metadata_state_batch_size: usize,
    pub metadata_gap_recheck_interval_secs: u64,
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
