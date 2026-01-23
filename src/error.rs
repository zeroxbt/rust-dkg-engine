use thiserror::Error;

/// Top-level application error that composes all subsystem errors
#[derive(Error, Debug)]
pub enum NodeError {
    /// Blockchain-related errors
    #[error("Blockchain error: {0}")]
    Blockchain(#[from] blockchain::error::BlockchainError),

    /// Network-related errors
    #[error("Network error: {0}")]
    Network(#[from] network::error::NetworkError),

    /// Database/repository errors
    #[error("Repository error: {0}")]
    Repository(#[from] repository::error::RepositoryError),

    /// Triple store errors
    #[error("Triple store error: {0}")]
    TripleStore(#[from] triple_store::error::TripleStoreError),

    /// I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// File service errors
    #[error("File service error: {0}")]
    FileService(#[from] crate::services::file_service::FileServiceError),

    /// Generic application error
    #[error("{0}")]
    Other(String),
}
