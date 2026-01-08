use thiserror::Error;

/// Error types for repository/database operations
#[derive(Error, Debug)]
pub enum RepositoryError {
    /// Database error - wraps all SeaORM errors
    #[error(transparent)]
    Database(#[from] sea_orm::DbErr),
}

/// Convenient Result type alias for RepositoryError
pub type Result<T> = std::result::Result<T, RepositoryError>;
