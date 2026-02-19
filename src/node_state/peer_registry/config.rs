use std::time::Duration;

// Performance tracking constants
pub(super) const HISTORY_SIZE: usize = 20;
pub(super) const FAILURE_LATENCY_MS: u64 = 15_000;
pub(super) const DEFAULT_LATENCY_MS: u64 = 5_000;

// Discovery backoff constants
pub(super) const DISCOVERY_BASE_DELAY: Duration = Duration::from_secs(60);
pub(super) const DISCOVERY_MAX_DELAY: Duration = Duration::from_secs(960);
