use std::sync::Arc;

use axum::body::Body;
use governor::middleware::NoOpMiddleware;
use serde::Deserialize;
use tower_governor::{
    GovernorLayer, governor::GovernorConfigBuilder, key_extractor::PeerIpKeyExtractor,
};

/// Configuration for the HTTP API rate limiter.
///
/// Uses a token bucket algorithm where tokens are replenished at a steady rate.
/// Clients can make burst requests up to `burst_size`, then are limited to
/// `max_requests` per `time_window`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct RateLimiterConfig {
    /// Whether rate limiting is enabled. Defaults to true.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Time window in seconds for rate limiting. Defaults to 60 seconds.
    #[serde(default = "default_time_window_seconds")]
    pub time_window_seconds: u64,

    /// Maximum number of requests allowed per time window. Defaults to 10.
    #[serde(default = "default_max_requests")]
    pub max_requests: u32,

    /// Burst capacity - maximum requests allowed in a burst before throttling.
    /// Defaults to max_requests if not specified.
    #[serde(default)]
    pub burst_size: Option<u32>,
}

fn default_enabled() -> bool {
    true
}

fn default_time_window_seconds() -> u64 {
    60
}

fn default_max_requests() -> u32 {
    10
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            time_window_seconds: default_time_window_seconds(),
            max_requests: default_max_requests(),
            burst_size: None,
        }
    }
}

impl RateLimiterConfig {
    /// Get the effective burst size (defaults to max_requests if not specified).
    pub(crate) fn effective_burst_size(&self) -> u32 {
        self.burst_size.unwrap_or(self.max_requests)
    }

    /// Build the rate limiter layer.
    ///
    /// Returns `None` if rate limiting is disabled.
    pub(crate) fn build_layer(
        &self,
    ) -> Option<GovernorLayer<PeerIpKeyExtractor, NoOpMiddleware, Body>> {
        if !self.enabled {
            return None;
        }

        // Calculate replenish interval: time_window / max_requests
        // e.g., 60 seconds / 10 requests = 6 seconds per token = 6000ms
        let replenish_interval_ms =
            (self.time_window_seconds * 1000) / u64::from(self.max_requests);

        let config = Arc::new(
            GovernorConfigBuilder::default()
                .per_millisecond(replenish_interval_ms)
                .burst_size(self.effective_burst_size())
                .finish()
                .expect("Failed to build governor config"),
        );

        Some(GovernorLayer::new(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RateLimiterConfig::default();
        assert!(config.enabled);
        assert_eq!(config.time_window_seconds, 60);
        assert_eq!(config.max_requests, 10);
        assert_eq!(config.burst_size, None);
        assert_eq!(config.effective_burst_size(), 10);
    }

    #[test]
    fn test_effective_burst_size_uses_max_requests_when_none() {
        let config = RateLimiterConfig {
            enabled: true,
            time_window_seconds: 60,
            max_requests: 20,
            burst_size: None,
        };
        assert_eq!(config.effective_burst_size(), 20);
    }

    #[test]
    fn test_effective_burst_size_uses_explicit_value() {
        let config = RateLimiterConfig {
            enabled: true,
            time_window_seconds: 60,
            max_requests: 20,
            burst_size: Some(5),
        };
        assert_eq!(config.effective_burst_size(), 5);
    }

    #[test]
    fn test_build_layer_returns_none_when_disabled() {
        let config = RateLimiterConfig {
            enabled: false,
            ..Default::default()
        };
        assert!(config.build_layer().is_none());
    }

    #[test]
    fn test_build_layer_returns_some_when_enabled() {
        let config = RateLimiterConfig::default();
        assert!(config.build_layer().is_some());
    }
}
