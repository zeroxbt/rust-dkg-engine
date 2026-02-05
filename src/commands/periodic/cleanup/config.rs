use serde::Deserialize;

/// Cleanup configuration for periodic maintenance tasks.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct CleanupConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub operations: OperationsCleanupConfig,
    pub pending_storage: PendingStorageCleanupConfig,
    pub finality_acks: FinalityAcksCleanupConfig,
    pub proof_challenges: ProofChallengesCleanupConfig,
    pub kc_sync_queue: KcSyncQueueCleanupConfig,
}

/// Cleanup config for operation status + result cache.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperationsCleanupConfig {
    pub ttl_secs: u64,
    pub batch_size: usize,
}

/// Cleanup config for pending storage (redb).
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingStorageCleanupConfig {
    pub ttl_secs: u64,
    pub batch_size: usize,
}

/// Cleanup config for finality ack records.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct FinalityAcksCleanupConfig {
    pub ttl_secs: u64,
    pub batch_size: usize,
}

/// Cleanup config for proof challenge records.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProofChallengesCleanupConfig {
    pub ttl_secs: u64,
    pub batch_size: usize,
}

/// Cleanup config for KC sync queue.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct KcSyncQueueCleanupConfig {
    pub ttl_secs: u64,
    pub max_retries: u32,
    pub batch_size: usize,
}
