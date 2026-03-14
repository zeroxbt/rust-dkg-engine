use alloy::{
    contract::Error as ContractError,
    signers::local::LocalSignerError,
    transports::{RpcError, TransportErrorKind},
};

#[derive(Debug, thiserror::Error)]
pub enum BlockchainError {
    #[error("Contract error: {0}")]
    Contract(#[from] ContractError),

    #[error("Invalid address: {address}")]
    InvalidAddress { address: String },

    #[error("Invalid private key (length: {key_length})")]
    InvalidPrivateKey {
        key_length: usize,
        #[source]
        source: LocalSignerError,
    },

    #[error("RPC connection failed after trying {attempts} endpoint(s)")]
    RpcConnectionFailed { attempts: usize },

    #[error("Blockchain '{blockchain_id}' not found")]
    BlockchainNotFound { blockchain_id: String },

    #[error("Invalid contract name: {name}")]
    InvalidContractName { name: String },

    #[error("Failed to get logs: {reason}")]
    GetLogs {
        reason: String,
        #[source]
        source: Option<RpcError<TransportErrorKind>>,
    },

    #[error("Failed to get block number: {reason}")]
    GetBlockNumber {
        reason: String,
        #[source]
        source: Option<RpcError<TransportErrorKind>>,
    },

    #[error("Profile creation failed: {reason}")]
    ProfileCreation { reason: String },

    #[error("Identity not found after profile creation")]
    IdentityNotFound,

    #[error("Identity ID not found")]
    IdentityIdNotFound,

    #[error("Management wallet private key required")]
    ManagementKeyRequired,

    #[error("Transaction failed: {contract}::{function} - {reason}")]
    TransactionFailed {
        contract: String,
        function: String,
        reason: String,
    },

    #[error("Transaction receipt failed: {reason}")]
    ReceiptFailed { reason: String },

    #[error("Hex decoding failed: {context}")]
    HexDecode {
        context: String,
        #[source]
        source: alloy::primitives::hex::FromHexError,
    },

    #[error("Signing failed: {reason}")]
    SigningFailed { reason: String },

    #[error("Provider initialization failed: {reason}")]
    ProviderInit { reason: String },

    #[error("Contract initialization failed: {reason}")]
    ContractInit { reason: String },

    #[error("Missing configuration value: {field}")]
    MissingConfig { field: &'static str },

    #[error("Contract '{contract}' is not initialized")]
    ContractNotInitialized { contract: String },

    #[error("Contract '{contract}' has no initialized instances")]
    ContractInstancesMissing { contract: String },

    #[error("RPC call failed for {operation}: {reason}")]
    RpcCallFailed { operation: String, reason: String },

    #[error("Invalid chain value for {field}: {value}")]
    InvalidChainValue { field: String, value: String },

    #[error("Value overflow for {field}")]
    ValueOverflow { field: String },

    #[error("Failed to load block {block_number} data: {reason}")]
    BlockData { block_number: u64, reason: String },

    #[error("Failed to look up transaction {tx_hash}: {reason}")]
    TransactionLookup { tx_hash: String, reason: String },

    #[error("Failed to resolve contract code at block {block_number}: {reason}")]
    CodeLookup { block_number: u64, reason: String },

    #[error("Substrate RPC error for endpoint '{endpoint}': {reason}")]
    SubstrateRpc { endpoint: String, reason: String },

    #[error("Substrate metadata error: {reason}")]
    SubstrateMetadata { reason: String },

    #[error("Failed to initialize blockchain '{blockchain_id}': {source}")]
    BlockchainInitialization {
        blockchain_id: String,
        #[source]
        source: Box<BlockchainError>,
    },

    #[error("Failed to initialize identity for blockchain '{blockchain_id}': {source}")]
    IdentityInitialization {
        blockchain_id: String,
        #[source]
        source: Box<BlockchainError>,
    },

    #[error("EVM account mapping required for {wallet_type} wallet {wallet_address}")]
    EvmAccountMappingRequired {
        wallet_type: String,
        wallet_address: String,
    },

    #[error("Duplicate blockchain_id '{blockchain_id}' found in configuration")]
    DuplicateBlockchainId { blockchain_id: String },

    #[error(
        "Invalid blockchain_id format '{blockchain_id}': expected 'chaintype:chainid' (e.g., 'hardhat:31337')"
    )]
    InvalidBlockchainId { blockchain_id: String },
}

impl BlockchainError {
    /// Create a GetLogs error with the underlying RPC error
    pub fn get_logs(err: RpcError<TransportErrorKind>) -> Self {
        Self::GetLogs {
            reason: err.to_string(),
            source: Some(err),
        }
    }

    /// Create a GetBlockNumber error with the underlying RPC error
    pub fn get_block_number(err: RpcError<TransportErrorKind>) -> Self {
        Self::GetBlockNumber {
            reason: err.to_string(),
            source: Some(err),
        }
    }
}
