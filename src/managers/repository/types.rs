use std::str::FromStr;

/// Operation status for tracking publish/get operation polling.
///
/// Used by the operation repository to track publish/get polling lifecycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationStatus {
    /// Operation is currently in progress
    InProgress,
    /// Operation completed successfully
    Completed,
    /// Operation failed
    Failed,
}

impl OperationStatus {
    /// Convert to database string representation.
    pub(crate) fn as_str(&self) -> &'static str {
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
