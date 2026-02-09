use serde::{Deserialize, Serialize};

use super::rate_limiter::PeerRateLimiterConfig;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RpcConfig {
    pub rate_limiter: PeerRateLimiterConfig,
}
