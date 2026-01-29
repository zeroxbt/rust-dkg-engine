use serde::{Deserialize, Serialize};

/// Maximum number of knowledge assets per collection.
/// Used to convert between global token IDs and local 1-based indices.
/// Global token ID formula: (kc_id - 1) * MAX_TOKENS_PER_KC + local_token_id
pub(crate) const MAX_TOKENS_PER_KC: u64 = 1_000_000;

/// Token ID range for knowledge assets within a collection.
///
/// Represents the range of knowledge asset token IDs, including
/// any burned (deleted) tokens that should be excluded from queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TokenIds {
    /// The starting token ID (inclusive)
    start_token_id: u64,
    /// The ending token ID (inclusive)
    end_token_id: u64,
    /// List of burned token IDs to exclude from the range
    burned: Vec<u64>,
}

impl TokenIds {
    /// Create a new token ID range.
    pub(crate) fn new(start_token_id: u64, end_token_id: u64, burned: Vec<u64>) -> Self {
        Self {
            start_token_id,
            end_token_id,
            burned,
        }
    }

    /// Create a range for a single token.
    pub(crate) fn single(token_id: u64) -> Self {
        Self {
            start_token_id: token_id,
            end_token_id: token_id,
            burned: vec![],
        }
    }

    /// Returns the starting token ID (inclusive).
    pub(crate) fn start_token_id(&self) -> u64 {
        self.start_token_id
    }

    /// Returns the ending token ID (inclusive).
    pub(crate) fn end_token_id(&self) -> u64 {
        self.end_token_id
    }

    /// Returns a reference to the list of burned token IDs.
    pub(crate) fn burned(&self) -> &[u64] {
        &self.burned
    }
}

impl Default for TokenIds {
    fn default() -> Self {
        Self::single(1)
    }
}
