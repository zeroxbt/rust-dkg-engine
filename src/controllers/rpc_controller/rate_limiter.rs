use std::{num::NonZeroU32, sync::Arc};

use dashmap::DashMap;
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use libp2p::PeerId;
use serde::Deserialize;

type PeerLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Configuration for the per-peer rate limiter.
///
/// Uses a token bucket algorithm where tokens are replenished at `requests_per_second` rate.
/// Peers can burst up to `burst_size` requests before being rate limited.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PeerRateLimiterConfig {
    /// Whether rate limiting is enabled.
    pub enabled: bool,

    /// Number of requests allowed per second (token refill rate).
    pub requests_per_second: u32,

    /// Maximum burst capacity before throttling.
    pub burst_size: u32,
}

/// Per-peer rate limiter for inbound RPC requests.
///
/// Each peer gets its own token bucket, shared across all protocols.
/// This prevents any single peer from overwhelming the node with requests.
pub(crate) struct PeerRateLimiter {
    limiters: DashMap<PeerId, Arc<PeerLimiter>>,
    config: PeerRateLimiterConfig,
}

impl PeerRateLimiter {
    pub(crate) fn new(config: PeerRateLimiterConfig) -> Self {
        tracing::info!(
            enabled = config.enabled,
            requests_per_second = config.requests_per_second,
            burst_size = config.burst_size,
            "Peer rate limiter initialized"
        );

        Self {
            limiters: DashMap::new(),
            config,
        }
    }

    /// Check if a request from a peer is allowed.
    ///
    /// Returns `true` if the request is allowed, `false` if rate limited.
    pub(crate) fn check(&self, peer: &PeerId) -> bool {
        if !self.config.enabled {
            return true;
        }

        let limiter = self.get_or_create_limiter(peer);
        let allowed = limiter.check().is_ok();

        if !allowed {
            tracing::debug!(
                peer = %peer,
                "Peer rate limited"
            );
        }

        allowed
    }

    fn get_or_create_limiter(&self, peer: &PeerId) -> Arc<PeerLimiter> {
        if let Some(limiter) = self.limiters.get(peer) {
            return Arc::clone(limiter.value());
        }

        // Create quota: burst_size requests, refilling at requests_per_second rate
        let burst = NonZeroU32::new(self.config.burst_size).unwrap_or(NonZeroU32::MIN);
        let rate = NonZeroU32::new(self.config.requests_per_second).unwrap_or(NonZeroU32::MIN);

        let quota = Quota::per_second(rate).allow_burst(burst);
        let limiter = Arc::new(RateLimiter::direct(quota));

        self.limiters.insert(*peer, Arc::clone(&limiter));
        limiter
    }
}
