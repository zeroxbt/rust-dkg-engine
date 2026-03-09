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
    pub max_contract_concurrency: usize,
    pub metadata_discovery_max_blocks_per_chunk: u64,
    pub metadata_state_max_kc_per_chunk: usize,
    pub metadata_error_retry_interval_secs: u64,
    pub live_poll_interval_secs: u64,
    pub queue_high_kc_watermark: u64,
    pub queue_low_kc_watermark: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct DkgSyncQueueProcessorConfig {
    /// Global cap for KCs simultaneously in-flight across all pipeline stages.
    pub inflight_kc_limit: usize,
    /// Max number of due queue rows dispatcher pulls in one dispatch attempt.
    pub dispatch_max_kc_per_attempt: usize,
    /// Per-channel message buffer between pipeline stages.
    pub stage_channel_message_buffer: usize,
    pub filter_max_kc_per_chunk: usize,
    pub fetch_max_kc_per_batch: usize,
    /// Number of fetch batches processed concurrently.
    ///
    /// `1` preserves previous behavior (sequential fetch batches).
    pub fetch_batch_concurrency: usize,
    pub fetch_peer_fanout_concurrency: usize,
    /// Optional hard cap on how many peers can be attempted for a single fetch batch.
    ///
    /// `None` means no cap (all eligible peers). `Some(n)` limits attempts to top `n` peers.
    pub max_peer_attempts_per_batch: Option<usize>,
    pub fetch_max_ka_per_batch: u64,
    pub insert_kc_concurrency: usize,

    pub dispatch_idle_poll_secs: u64,
    pub max_retry_attempts: u32,
}
