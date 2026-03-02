use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct DkgSyncConfig {
    pub discovery: DkgSyncDiscoveryConfig,
    pub queue_processor: DkgSyncQueueProcessorConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct DkgSyncDiscoveryConfig {
    pub enabled: bool,
    pub head_safety_blocks: u64,
    pub max_contract_concurrency: usize,
    pub metadata_discovery_block_batch_size: u64,
    pub metadata_state_batch_size: usize,
    pub metadata_error_retry_interval_secs: u64,
    pub queue_high_watermark: u64,
    pub queue_low_watermark: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct DkgSyncQueueProcessorConfig {
    pub pipeline_capacity: usize,
    pub pipeline_channel_buffer: usize,
    pub filter_batch_size: usize,
    pub max_assets_per_fetch_batch: u64,
    pub insert_batch_concurrency: usize,

    pub dispatch_idle_poll_secs: u64,
    pub max_retry_attempts: u32,
}
