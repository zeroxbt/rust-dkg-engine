use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct SyncConfig {
    pub enabled: bool,
    pub head_safety_blocks: u64,
    pub metadata_backfill_block_batch_size: u64,
    pub metadata_state_batch_size: usize,
    pub metadata_gap_recheck_interval_secs: u64,
    pub no_peers_retry_delay_secs: u64,
    pub max_retry_attempts: u32,
    pub max_new_kcs_per_contract: u64,
    pub pipeline_capacity: usize,
    pub pipeline_channel_buffer: usize,
    pub max_assets_per_fetch_batch: u64,
    pub insert_batch_concurrency: usize,
    pub retry_base_delay_secs: u64,
    pub retry_max_delay_secs: u64,
    pub retry_jitter_secs: u64,
}
