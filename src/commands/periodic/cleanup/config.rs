use serde::Deserialize;

/// Cleanup configuration for periodic maintenance tasks.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct CleanupConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub operations: OperationsCleanupConfig,
    pub publish_tmp_dataset: PublishTmpDatasetCleanupConfig,
    pub finality_acks: FinalityAcksCleanupConfig,
    pub proof_challenges: ProofChallengesCleanupConfig,
}

/// Cleanup config for operation status + result cache.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperationsCleanupConfig {
    pub ttl_secs: u64,
    pub batch_size: usize,
}

/// Cleanup config for publish tmp dataset store (redb).
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublishTmpDatasetCleanupConfig {
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
