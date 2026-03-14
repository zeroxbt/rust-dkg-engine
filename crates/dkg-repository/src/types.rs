use std::str::FromStr;

use thiserror::Error;

/// Operation status for tracking publish/get operation polling.
///
/// Used by the operation repository to track publish/get polling lifecycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    /// Operation is currently in progress
    InProgress,
    /// Operation completed successfully
    Completed,
    /// Operation failed
    Failed,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KcProjectionDesiredState {
    Present = 1,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KcProjectionActualState {
    Unknown = 0,
    Present = 1,
    Failed = 2,
    Pending = 3,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("invalid KC projection desired state value: {0}")]
pub struct KcProjectionDesiredStateParseError(pub u8);

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("invalid KC projection actual state value: {0}")]
pub struct KcProjectionActualStateParseError(pub u8);

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("'{0}' is not a valid operation status")]
pub struct OperationStatusParseError(pub String);

/// Public DTO for paranet knowledge collection sync queue entries.
///
/// This type is intentionally decoupled from SeaORM model internals so
/// application code does not depend on persistence-layer structs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParanetKcSyncEntry {
    pub paranet_ual: String,
    pub kc_ual: String,
    pub retry_count: u32,
}

/// Public DTO for operation records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRecord {
    pub operation_id: String,
    pub operation_name: String,
    pub status: String,
    pub error_message: Option<String>,
}

impl OperationRecord {
    /// Parse the status string into an OperationStatus enum.
    pub fn operation_status(&self) -> Result<OperationStatus, OperationStatusParseError> {
        OperationStatus::from_str(&self.status)
    }
}

/// Public DTO for KC sync queue rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KcSyncQueueEntry {
    pub blockchain_id: String,
    pub contract_address: String,
    pub kc_id: u64,
    pub retry_count: u32,
    pub next_retry_at: i64,
    pub created_at: i64,
    pub last_retry_at: Option<i64>,
}

/// Public DTO for canonical KC chain metadata rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KcChainMetadataEntry {
    pub blockchain_id: String,
    pub contract_address: String,
    pub kc_id: u64,
    pub publisher_address: String,
    pub block_number: u64,
    pub transaction_hash: String,
    pub block_timestamp: u64,
    pub range_start_token_id: Option<u64>,
    pub range_end_token_id: Option<u64>,
    pub burned_mode: Option<u32>,
    pub burned_payload: Option<Vec<u8>>,
    pub end_epoch: Option<u64>,
    pub latest_merkle_root: Option<String>,
    pub state_observed_block: Option<u64>,
    pub state_updated_at: i64,
    pub private_graph_mode: Option<u32>,
    pub private_graph_payload: Option<Vec<u8>>,
    pub publish_operation_id: Option<String>,
    pub source: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// KC metadata row with full core metadata and sync state available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KcChainReadyKcStateMetadataEntry {
    pub blockchain_id: String,
    pub contract_address: String,
    pub kc_id: u64,
    pub publisher_address: String,
    pub block_number: u64,
    pub transaction_hash: String,
    pub block_timestamp: u64,
    pub range_start_token_id: u64,
    pub range_end_token_id: u64,
    pub burned_mode: u32,
    pub burned_payload: Vec<u8>,
    pub end_epoch: u64,
    pub latest_merkle_root: String,
    pub state_observed_block: u64,
}

/// Input DTO for KC state metadata upsert in sync metadata pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncMetadataStateInput {
    pub range_start_token_id: u64,
    pub range_end_token_id: u64,
    pub burned_mode: u32,
    pub burned_payload: Vec<u8>,
    pub end_epoch: u64,
    pub latest_merkle_root: String,
}

/// Input DTO for atomic metadata+queue persistence in sync metadata pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncMetadataRecordInput {
    pub kc_id: u64,
    pub publisher_address: String,
    pub block_number: u64,
    pub transaction_hash: String,
    pub block_timestamp: u64,
    pub publish_operation_id: String,
    pub source: String,
    pub state: Option<SyncMetadataStateInput>,
}

/// Public DTO for proof challenge rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofChallengeEntry {
    pub blockchain_id: String,
    pub epoch: i64,
    pub proof_period_start_block: i64,
    pub contract_address: String,
    pub knowledge_collection_id: i64,
    pub chunk_index: i64,
    pub state: String,
    pub score: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl OperationStatus {
    /// Convert to database string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InProgress => "IN_PROGRESS",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

impl From<KcProjectionDesiredState> for u8 {
    fn from(value: KcProjectionDesiredState) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for KcProjectionDesiredState {
    type Error = KcProjectionDesiredStateParseError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Present),
            _ => Err(KcProjectionDesiredStateParseError(value)),
        }
    }
}

impl From<KcProjectionActualState> for u8 {
    fn from(value: KcProjectionActualState) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for KcProjectionActualState {
    type Error = KcProjectionActualStateParseError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::Present),
            2 => Ok(Self::Failed),
            3 => Ok(Self::Pending),
            _ => Err(KcProjectionActualStateParseError(value)),
        }
    }
}

impl std::fmt::Display for OperationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Parse from database string representation.
///
impl FromStr for OperationStatus {
    type Err = OperationStatusParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "IN_PROGRESS" => Ok(Self::InProgress),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            _ => Err(OperationStatusParseError(s.to_string())),
        }
    }
}
