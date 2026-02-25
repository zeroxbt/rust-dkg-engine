//! DKG ontology predicates.
//!
//! These predicates are used for RDF-star annotations and metadata
//! in the OriginTrail Decentralized Knowledge Graph.

/// Predicate linking a KC to its named graphs
pub const HAS_NAMED_GRAPH: &str = "https://ontology.origintrail.io/dkg/1.0#hasNamedGraph";

/// Predicate linking a KC to its knowledge assets
pub const HAS_KNOWLEDGE_ASSET: &str = "https://ontology.origintrail.io/dkg/1.0#hasKnowledgeAsset";

/// Metadata: publisher identity
pub const PUBLISHED_BY: &str = "https://ontology.origintrail.io/dkg/1.0#publishedBy";

/// Metadata: block number when published
pub const PUBLISHED_AT_BLOCK: &str = "https://ontology.origintrail.io/dkg/1.0#publishedAtBlock";

/// Metadata: transaction hash of publish operation
pub const PUBLISH_TX: &str = "https://ontology.origintrail.io/dkg/1.0#publishTx";

/// Metadata: block timestamp
pub const BLOCK_TIME: &str = "https://ontology.origintrail.io/dkg/1.0#blockTime";

/// Predicate for private assertion merkle root
pub const PRIVATE_MERKLE_ROOT: &str = "https://ontology.origintrail.io/dkg/1.0#privateMerkleRoot";

/// Prefix for publisher key DID in metadata triples
pub const PUBLISHER_KEY_PREFIX: &str = "did:dkg:publisherKey/";
