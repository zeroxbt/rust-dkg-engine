use std::num::NonZeroU32;

use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};

/// Rate limiter for RPC calls to prevent exceeding provider limits.
///
/// This wraps the `governor` crate to provide simple rate limiting based on
/// requests per second. If no limit is configured, all calls pass through immediately.
pub(crate) struct RpcRateLimiter {
    limiter: Option<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

impl RpcRateLimiter {
    /// Create a new rate limiter.
    ///
    /// - `requests_per_second`: Maximum requests per second. `None` means unlimited.
    pub(crate) fn new(requests_per_second: Option<u32>) -> Self {
        let limiter = requests_per_second.and_then(|rps| {
            NonZeroU32::new(rps).map(|rps| {
                let quota = Quota::per_second(rps);
                RateLimiter::direct(quota)
            })
        });

        Self { limiter }
    }

    /// Wait until a request can be made according to the rate limit.
    ///
    /// If no rate limit is configured, this returns immediately.
    pub(crate) async fn acquire(&self) {
        if let Some(limiter) = &self.limiter {
            limiter.until_ready().await;
        }
    }
}
