use thiserror::Error;

/// An error type for failures that may occur during the canonicalization process.
///
/// This enum captures different categories of errors that can occur in the URDNA2015 algorithm,
/// including parsing input, hashing intermediate data, and serializing output.
#[derive(Error, Debug)]
pub enum URDNAError {
    #[error("Parsing error: {0}")]
    Parsing(String),

    #[error("Hashing error: {0}")]
    Hashing(String),

    #[error("Serialization error: {0}")]
    Serializing(String),
}
