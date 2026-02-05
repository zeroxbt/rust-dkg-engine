use std::time::Duration;

use crate::managers::blockchain::error_classification::{
    contract_error_backoff_hint, is_retryable_contract_error, is_retryable_rpc_error,
    rpc_backoff_hint, should_refresh_contract_error, should_refresh_rpc_error,
};

pub(crate) struct RetryPolicy {
    pub max_attempts: usize,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl RetryPolicy {
    pub(crate) fn rpc_default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(2),
        }
    }

    pub(crate) fn tx_default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(5),
        }
    }
}

pub(crate) trait RetryableError: std::fmt::Display {
    fn is_retryable(&self) -> bool;
    fn backoff_hint(&self) -> Option<Duration> {
        None
    }
    fn should_refresh_provider(&self) -> bool {
        false
    }
}

impl RetryableError for alloy::transports::RpcError<alloy::transports::TransportErrorKind> {
    fn is_retryable(&self) -> bool {
        is_retryable_rpc_error(self)
    }

    fn backoff_hint(&self) -> Option<Duration> {
        rpc_backoff_hint(self)
    }

    fn should_refresh_provider(&self) -> bool {
        should_refresh_rpc_error(self)
    }
}

impl RetryableError for alloy::contract::Error {
    fn is_retryable(&self) -> bool {
        is_retryable_contract_error(self)
    }

    fn backoff_hint(&self) -> Option<Duration> {
        contract_error_backoff_hint(self)
    }

    fn should_refresh_provider(&self) -> bool {
        should_refresh_contract_error(self)
    }
}

pub(crate) fn backoff_delay(
    policy: &RetryPolicy,
    attempt: usize,
    hint: Option<Duration>,
) -> Duration {
    if let Some(hint) = hint {
        return hint.min(policy.max_delay);
    }

    let base_ms = policy.base_delay.as_millis() as u64;
    let exponent = (attempt.saturating_sub(1)).min(6) as u32;
    let factor = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
    let delay_ms = base_ms.saturating_mul(factor);
    let max_ms = policy.max_delay.as_millis() as u64;

    Duration::from_millis(delay_ms.min(max_ms))
}
