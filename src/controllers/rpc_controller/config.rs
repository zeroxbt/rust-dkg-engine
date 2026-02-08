use serde::Deserialize;

use super::rate_limiter::PeerRateLimiterConfig;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RpcConfig {
    pub rate_limiter: PeerRateLimiterConfig,
}
