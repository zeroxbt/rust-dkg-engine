use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProtocolOperation {
    Publish,
    Get,
    Update,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationStatus {
    InProgress,
    Completed,
    Failed,
}

impl OperationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InProgress => "IN_PROGRESS",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "IN_PROGRESS" => Self::InProgress,
            "COMPLETED" => Self::Completed,
            "FAILED" => Self::Failed,
            _ => Self::InProgress, // Default to InProgress for unknown values
        }
    }
}
