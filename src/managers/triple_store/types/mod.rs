mod knowledge_asset;

// Re-export shared domain types from crate::types
// Local types specific to triple store operations
pub(crate) use knowledge_asset::KnowledgeAsset;

pub(crate) use crate::types::{Assertion, MAX_TOKENS_PER_KC, TokenIds};

/// Visibility for triple store graph operations.
///
/// Unlike `Visibility` (application level, includes `All`), this enum
/// represents the actual named graph suffix used in SPARQL queries.
/// The triple store manager operates on individual graphs, so it only
/// accepts `Public` or `Private` - never both at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraphVisibility {
    /// Public named graph (`{ual}/public`)
    Public,
    /// Private named graph (`{ual}/private`)
    Private,
}

impl GraphVisibility {
    /// Get the graph suffix string for SPARQL queries
    pub(crate) fn as_suffix(&self) -> &'static str {
        match self {
            GraphVisibility::Public => "public",
            GraphVisibility::Private => "private",
        }
    }
}
