use thiserror::Error;

#[derive(Error, Debug)]
pub enum KeyValueStoreError {
    #[error("Database error: {0}")]
    Database(#[from] redb::Error),

    #[error("Database error: {0}")]
    DatabaseCreate(#[from] redb::DatabaseError),

    #[error("Database commit error: {0}")]
    Commit(#[from] redb::CommitError),

    #[error("Database transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),

    #[error("Table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("Storage error: {0}")]
    Storage(#[from] redb::StorageError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
