use std::time::Duration;

use thiserror::Error;

/// Triple store specific errors
#[derive(Error, Debug)]
pub enum TripleStoreError {
    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// Triple store backend returned an error response
    #[error("Triple store error (status {status}): {message}")]
    Backend { status: u16, message: String },

    /// Query timed out
    #[error("Query timed out after {}ms", timeout.as_millis())]
    Timeout { timeout: Duration },

    /// Failed to connect after multiple retries
    #[error("Failed to connect to triple store after {attempts} attempts")]
    ConnectionFailed { attempts: u32 },

    /// Repository not found
    #[error("Repository '{name}' not found")]
    RepositoryNotFound { name: String },

    /// Failed to parse response
    #[error("Failed to parse response: {reason}")]
    ParseError { reason: String },

    /// Knowledge collection or asset not found
    #[error("{entity} not found with UAL: {ual}")]
    NotFound { entity: String, ual: String },

    /// Invalid SPARQL query
    #[error("Invalid SPARQL query: {reason}")]
    InvalidQuery { reason: String },

    /// Generic error
    #[error("{0}")]
    Other(String),
}

/// Convenient Result type alias
pub type Result<T> = std::result::Result<T, TripleStoreError>;
