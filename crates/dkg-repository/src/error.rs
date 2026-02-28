use thiserror::Error;

/// Error types for repository/database operations
#[derive(Error, Debug)]
pub enum RepositoryError {
    /// Database error - wraps all SeaORM errors
    #[error(transparent)]
    Database(#[from] sea_orm::DbErr),

    /// Record not found error
    #[error("Record not found: {0}")]
    NotFound(String),

    /// Invalid sync metadata payload for persistence.
    #[error("Invalid sync metadata: {0}")]
    SyncMetadata(String),
}

/// Convenient Result type alias for RepositoryError
pub type Result<T> = std::result::Result<T, RepositoryError>;
