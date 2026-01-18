use alloy::{contract::Error as ContractError, signers::local::LocalSignerError};

#[derive(Debug, thiserror::Error)]
pub enum BlockchainError {
    #[error("Contract error: {0}")]
    Contract(#[from] ContractError),

    #[error("Failed to decode message")]
    Decode,

    #[error("Invalid address. Address: {address}")]
    InvalidAddress { address: String },

    #[error("Invalid private key. Key length: {key_length}")]
    InvalidPrivateKey {
        key_length: usize,
        #[source]
        #[allow(unused)]
        source: LocalSignerError,
    },

    #[error("RPC connection failed after trying {attempts} endpoint(s)")]
    RpcConnectionFailed { attempts: usize },

    #[error("HTTP provider creation failed. Endpoint: {endpoint}")]
    HttpProviderCreation {
        endpoint: String,
        #[source]
        #[allow(unused)]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Blockchain not found. Blockchain: {blockchain}")]
    BlockchainNotFound { blockchain: String },

    #[error("Event '{event_name}' not found in contract")]
    EventNotFound { event_name: String },

    #[error("Failed to get logs")]
    GetLogs,

    #[error("Failed to get block number")]
    GetBlockNumber,

    #[error("Profile creation failed")]
    ProfileCreation,

    #[error("Identity not found after profile creation")]
    IdentityNotFound,

    #[error("Transaction failed: {contract}::{function} - {reason}")]
    TransactionFailed {
        contract: String,
        function: String,
        reason: String,
    },

    #[error("Contract reverted: {message}")]
    Revert { message: String },

    #[error("Contract panicked: {reason}")]
    Panic { reason: &'static str },

    #[error("{0}")]
    Custom(String),
}
