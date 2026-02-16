use serde::{Deserialize, Serialize};

/// Maximum number of knowledge assets per collection.
/// Used to convert between global token IDs and local 1-based indices.
/// Global token ID formula: (kc_id - 1) * MAX_TOKENS_PER_KC + local_token_id
pub const MAX_TOKENS_PER_KC: u64 = 1_000_000;

/// Token ID range for knowledge assets within a collection.
///
/// Represents the range of knowledge asset token IDs, including
/// any burned (deleted) tokens that should be excluded from queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenIds {
    /// The starting token ID (inclusive)
    start_token_id: u64,
    /// The ending token ID (inclusive)
    end_token_id: u64,
    /// List of burned token IDs to exclude from the range
    burned: Vec<u64>,
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

    /// Create token IDs from global (on-chain) range by converting to local 1-based indices.
    ///
    /// On-chain token IDs use a global formula: `(kc_id - 1) * 1_000_000 + local_token_id`
    /// This method converts back to local indices for use in triple store queries.
    pub fn from_global_range(
        knowledge_collection_id: u128,
        global_start: u64,
        global_end: u64,
        global_burned: Vec<u64>,
    ) -> Self {
        let offset = (knowledge_collection_id as u64 - 1) * MAX_TOKENS_PER_KC;
        let local_start = global_start.saturating_sub(offset);
        let local_end = global_end.saturating_sub(offset);
        let local_burned: Vec<u64> = global_burned
            .into_iter()
            .map(|b| b.saturating_sub(offset))
            .collect();

        Self::new(local_start, local_end, local_burned)
    }

    /// Returns the starting token ID (inclusive).
    pub fn start_token_id(&self) -> u64 {
        self.start_token_id
    }

    /// Returns the ending token ID (inclusive).
    pub fn end_token_id(&self) -> u64 {
        self.end_token_id
    }

    /// Returns a reference to the list of burned token IDs.
    pub fn burned(&self) -> &[u64] {
        &self.burned
    }
}

impl Default for TokenIds {
    fn default() -> Self {
        Self::single(1)
    }
}
