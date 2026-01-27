mod knowledge_asset;

// Re-export shared domain types from crate::types
// Local types specific to triple store operations
pub(crate) use knowledge_asset::KnowledgeAsset;

pub(crate) use crate::types::{Assertion, MAX_TOKENS_PER_KC, TokenIds, Visibility};
