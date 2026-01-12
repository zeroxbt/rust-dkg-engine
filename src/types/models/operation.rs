use std::{
    fmt::{self, Display},
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationId(Uuid);

impl OperationId {
    /// Generate a new operation ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Get the inner UUID (useful for database operations)
    pub fn into_inner(self) -> Uuid {
        self.0
    }
}

impl Default for OperationId {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for OperationId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

impl From<Uuid> for OperationId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<OperationId> for Uuid {
    fn from(id: OperationId) -> Self {
        id.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ProtocolOperation {
    Publish,
    Get,
    Update,
}
