use std::time::Duration;

use alloy::{
    contract::Error as ContractError,
    providers::PendingTransactionError,
    transports::{RpcError, TransportErrorKind},
};

const BUMP_GAS_PATTERNS: [&str; 10] = [
    "replacement transaction underpriced",
    "transaction underpriced",
    "fee too low",
    "max fee per gas less than block base fee",
    "max fee per gas less than block basefee",
    "priority fee too low",
    "nonce too low",
    "nonce is too low",
    "nonce has already been used",
    "already known",
];

pub fn is_retryable_rpc_error(err: &RpcError<TransportErrorKind>) -> bool {
    match err {
        RpcError::Transport(kind) => match kind {
            TransportErrorKind::MissingBatchResponse(_) => true,
            TransportErrorKind::BackendGone => true,
            TransportErrorKind::HttpError(http) => {
                http.is_rate_limit_err() || http.is_temporarily_unavailable()
            }
            TransportErrorKind::Custom(custom) => {
                let msg = custom.to_string().to_ascii_lowercase();
                msg.contains("too many requests") || msg.contains("rate limit")
            }
            _ => false,
        },
        RpcError::ErrorResp(payload) => payload.is_retry_err(),
        RpcError::NullResp => true,
        RpcError::DeserError { text, .. } => {
            let lowered = text.to_ascii_lowercase();
            lowered.contains("rate limit")
                || lowered.contains("too many requests")
                || lowered.contains("request limit")
        }
        _ => false,
    }
}

pub fn should_refresh_rpc_error(err: &RpcError<TransportErrorKind>) -> bool {
    matches!(
        err,
        RpcError::Transport(TransportErrorKind::BackendGone)
            | RpcError::Transport(TransportErrorKind::PubsubUnavailable)
    )
}

pub fn rpc_backoff_hint(err: &RpcError<TransportErrorKind>) -> Option<Duration> {
    let RpcError::ErrorResp(payload) = err else {
        return None;
    };

    let data = payload.try_data_as::<serde_json::Value>()?;
    let Ok(data) = data else {
        return None;
    };

    let backoff_seconds = data["rate"]["backoff_seconds"].as_f64()?;
    Some(Duration::from_secs(backoff_seconds.ceil() as u64))
}

pub fn is_retryable_pending_tx_error(err: &PendingTransactionError) -> bool {
    match err {
        PendingTransactionError::TransportError(inner) => is_retryable_rpc_error(inner),
        _ => false,
    }
}

pub fn pending_tx_backoff_hint(err: &PendingTransactionError) -> Option<Duration> {
    match err {
        PendingTransactionError::TransportError(inner) => rpc_backoff_hint(inner),
        _ => None,
    }
}

pub fn should_refresh_pending_tx_error(err: &PendingTransactionError) -> bool {
    match err {
        PendingTransactionError::TransportError(inner) => should_refresh_rpc_error(inner),
        _ => false,
    }
}

pub fn is_retryable_contract_error(err: &ContractError) -> bool {
    if err.as_revert_data().is_some() {
        return false;
    }

    match err {
        ContractError::TransportError(inner) => is_retryable_rpc_error(inner),
        ContractError::PendingTransactionError(inner) => is_retryable_pending_tx_error(inner),
        _ => false,
    }
}

pub fn contract_error_backoff_hint(err: &ContractError) -> Option<Duration> {
    match err {
        ContractError::TransportError(inner) => rpc_backoff_hint(inner),
        ContractError::PendingTransactionError(inner) => pending_tx_backoff_hint(inner),
        _ => None,
    }
}

pub fn should_refresh_contract_error(err: &ContractError) -> bool {
    match err {
        ContractError::TransportError(inner) => should_refresh_rpc_error(inner),
        ContractError::PendingTransactionError(inner) => should_refresh_pending_tx_error(inner),
        _ => false,
    }
}

pub fn should_bump_gas_price(err: &ContractError) -> bool {
    if err.as_revert_data().is_some() {
        return false;
    }

    let message = contract_error_message(err).to_ascii_lowercase();

    BUMP_GAS_PATTERNS
        .iter()
        .any(|pattern| message.contains(pattern))
}

fn contract_error_message(err: &ContractError) -> String {
    match err {
        ContractError::TransportError(inner) => rpc_error_message(inner),
        ContractError::PendingTransactionError(inner) => inner.to_string(),
        _ => err.to_string(),
    }
}

fn rpc_error_message(err: &RpcError<TransportErrorKind>) -> String {
    match err {
        RpcError::ErrorResp(payload) => payload.to_string(),
        RpcError::Transport(TransportErrorKind::HttpError(http)) => http.body.clone(),
        RpcError::Transport(TransportErrorKind::Custom(custom)) => custom.to_string(),
        RpcError::DeserError { text, .. } => text.clone(),
        _ => err.to_string(),
    }
}
