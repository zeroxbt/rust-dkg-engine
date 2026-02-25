use std::str::FromStr;

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
    pub fn operation_status(&self) -> Result<OperationStatus, String> {
        OperationStatus::from_str(&self.status)
    }
}

/// Public DTO for KC sync progress rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KcSyncProgressEntry {
    pub blockchain_id: String,
    pub contract_address: String,
    pub last_checked_id: u64,
    pub updated_at: i64,
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
    pub publish_operation_id: Option<String>,
    pub source: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
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

impl std::fmt::Display for OperationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Parse from database string representation.
///
/// Unknown values default to `InProgress`.
impl FromStr for OperationStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "IN_PROGRESS" => Ok(Self::InProgress),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            _ => Err(format!("'{}' is not a valid operation status", s)),
        }
    }
}
