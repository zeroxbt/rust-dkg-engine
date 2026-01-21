use serde::{Deserialize, Serialize};

/// Token ID range for knowledge assets within a collection.
///
/// Represents the range of knowledge asset token IDs, including
/// any burned (deleted) tokens that should be excluded from queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenIds {
    /// The starting token ID (inclusive)
    pub start_token_id: u64,
    /// The ending token ID (inclusive)
    pub end_token_id: u64,
    /// List of burned token IDs to exclude from the range
    pub burned: Vec<u64>,
}

impl TokenIds {
    /// Create a new token ID range.
    pub fn new(start_token_id: u64, end_token_id: u64, burned: Vec<u64>) -> Self {
        Self {
            start_token_id,
            end_token_id,
            burned,
        }
    }

    /// Create a range for a single token.
    pub fn single(token_id: u64) -> Self {
        Self {
            start_token_id: token_id,
            end_token_id: token_id,
            burned: vec![],
        }
    }

    /// Check if this represents a single token.
    pub fn is_single(&self) -> bool {
        self.start_token_id == self.end_token_id && self.burned.is_empty()
    }

    /// Get the count of active (non-burned) tokens in the range.
    pub fn active_count(&self) -> u64 {
        let total = self.end_token_id - self.start_token_id + 1;
        total.saturating_sub(self.burned.len() as u64)
    }

    /// Iterate over active token IDs (excluding burned).
    pub fn iter_active(&self) -> impl Iterator<Item = u64> + '_ {
        (self.start_token_id..=self.end_token_id).filter(|id| !self.burned.contains(id))
    }
}

impl Default for TokenIds {
    fn default() -> Self {
        Self::single(1)
    }
}
