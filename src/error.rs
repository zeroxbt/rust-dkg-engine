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

    /// Service layer errors
    #[error("Service error: {0}")]
    Service(ServiceError),

    /// Command execution errors
    #[error("Command error: {0}")]
    Command(CommandError),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

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

/// Service-specific errors
#[derive(Error, Debug)]
pub enum ServiceError {
    #[error("Operation not found: {0}")]
    OperationNotFound(String),

    #[error("Invalid operation state: {0}")]
    InvalidState(String),

    #[error("Insufficient responses: expected {expected}, got {actual}")]
    InsufficientResponses { expected: usize, actual: usize },

    #[error("Timeout waiting for responses")]
    Timeout,

    #[error("Invalid assertion: {0}")]
    InvalidAssertion(String),

    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    #[error("Sharding table error: {0}")]
    ShardingTable(String),

    #[error("Service error: {0}")]
    Other(String),
}

/// Command execution errors
#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Command not found: {0}")]
    NotFound(String),

    #[error("Command execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Command timeout")]
    Timeout,

    #[error("Command already running: {0}")]
    AlreadyRunning(String),

    #[error("Invalid command data: {0}")]
    InvalidData(String),

    #[error("Command scheduling failed: {0}")]
    SchedulingFailed(String),

    #[error("Command error: {0}")]
    Other(String),
}

/// Convenient Result type alias for NodeError
pub type Result<T> = std::result::Result<T, NodeError>;
