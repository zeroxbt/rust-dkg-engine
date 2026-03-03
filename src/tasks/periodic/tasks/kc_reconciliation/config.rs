use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct KcReconciliationConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub max_kc_rows_per_phase: usize,
}
