mod knowledge_asset;

// Re-export shared domain types from crate::types
pub(crate) use crate::types::{Assertion, TokenIds, Visibility, MAX_TOKENS_PER_KC};

// Local types specific to triple store operations
pub(crate) use knowledge_asset::KnowledgeAsset;
