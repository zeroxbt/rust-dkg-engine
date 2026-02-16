use thiserror::Error;

/// Top-level application error that composes all subsystem errors
#[derive(Error, Debug)]
pub(crate) enum NodeError {
    /// Blockchain-related errors
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] crate::managers::blockchain::error::BlockchainError),

    /// Network-related errors
    #[error("Network error: {0}")]
    Network(#[from] crate::managers::network::NetworkError),

    /// Database/repository errors
    #[error("Repository error: {0}")]
    Repository(#[from] crate::managers::repository::error::RepositoryError),

    /// Triple store errors
    #[error("Triple store error: {0}")]
    TripleStore(#[from] crate::managers::triple_store::error::TripleStoreError),

    /// I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Generic application error
    #[error("{0}")]
    Other(String),
}

impl From<dkg_key_value_store::ResultStoreError> for NodeError {
    fn from(err: dkg_key_value_store::ResultStoreError) -> Self {
        Self::Other(err.to_string())
    }
}
