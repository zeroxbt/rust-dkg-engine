use thiserror::Error;

/// Triple store specific errors
#[derive(Error, Debug)]
pub enum TripleStoreError {
    /// Semaphore closed
    #[error("Semaphore closed")]
    SemaphoreClosed,

    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// I/O operation failed
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Triple store backend returned an error response
    #[error("Triple store error (status {status}): {message}")]
    Backend { status: u16, message: String },

    /// Failed to connect after multiple retries
    #[error("Failed to connect to triple store after {attempts} attempts")]
    ConnectionFailed { attempts: u32 },

    /// Failed to parse response
    #[error("Failed to parse response: {reason}")]
    ParseError { reason: String },

    /// Invalid SPARQL query
    #[error("Invalid SPARQL query: {reason}")]
    InvalidQuery { reason: String },

    /// Generic error
    #[error("{0}")]
    Other(String),
}

/// Convenient Result type alias
pub type Result<T> = std::result::Result<T, TripleStoreError>;
