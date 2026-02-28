use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct SyncConfig {
    pub enabled: bool,
    pub head_safety_blocks: u64,

    // Replenisher
    pub metadata_backfill_block_batch_size: u64,
    pub metadata_state_batch_size: usize,
    pub metadata_error_retry_interval_secs: u64,
    pub queue_high_watermark: u64,
    pub queue_low_watermark: u64,

    // Pipeline / dispatch
    pub pipeline_capacity: usize,
    pub stage_channel_buffer: usize,
    pub filter_batch_size: usize,
    pub max_assets_per_fetch_batch: u64,
    pub insert_batch_concurrency: usize,

    // Retry
    pub dispatch_idle_poll_secs: u64,
    pub max_retry_attempts: u32,
}
