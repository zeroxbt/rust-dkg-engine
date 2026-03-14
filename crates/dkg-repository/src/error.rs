use thiserror::Error;
use uuid::Uuid;

/// Error types for repository/database operations
#[derive(Error, Debug)]
pub enum RepositoryError {
    /// Database error - wraps all SeaORM errors
    #[error(transparent)]
    Database(#[from] sea_orm::DbErr),

    /// Operation record not found
    #[error("Operation {operation_id} not found")]
    OperationNotFound { operation_id: Uuid },

    /// Proof challenge record not found after creation
    #[error(
        "Proof challenge not found after creation: blockchain_id={blockchain_id}, epoch={epoch}, proof_period_start_block={proof_period_start_block}"
    )]
    ProofChallengeNotFound {
        blockchain_id: String,
        epoch: i64,
        proof_period_start_block: i64,
    },

    /// KC chain state metadata row not found
    #[error(
        "KC chain state metadata row not found: blockchain_id={blockchain_id}, contract_address={contract_address}, kc_id={kc_id}"
    )]
    KcChainStateMetadataNotFound {
        blockchain_id: String,
        contract_address: String,
        kc_id: u64,
    },

    /// Invalid sync metadata payload for persistence.
    #[error("Sync metadata value overflow: field={field}, value={value}, target={target_type}")]
    SyncMetadataOverflow {
        field: &'static str,
        value: u64,
        target_type: &'static str,
    },
}

/// Convenient Result type alias for RepositoryError
pub type Result<T> = std::result::Result<T, RepositoryError>;
