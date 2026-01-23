//! DKG ontology predicates.
//!
//! These predicates are used for RDF-star annotations and metadata
//! in the OriginTrail Decentralized Knowledge Graph.

/// Base URI for the DKG ontology
pub const DKG_BASE: &str = "https://ontology.origintrail.io/dkg/1.0#";

/// UAL predicate for triple annotations (identifies which asset a triple belongs to)
pub const UAL: &str = "https://ontology.origintrail.io/dkg/1.0#UAL";

/// Label predicate for visibility annotations ("private" label marks private triples)
pub const LABEL: &str = "https://ontology.origintrail.io/dkg/1.0#label";

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

/// Metadata: timestamp when published
pub const PUBLISH_TIME: &str = "https://ontology.origintrail.io/dkg/1.0#publishTime";

/// Metadata: block timestamp
pub const BLOCK_TIME: &str = "https://ontology.origintrail.io/dkg/1.0#blockTime";

/// Predicate for private assertion merkle root
pub const PRIVATE_MERKLE_ROOT: &str = "https://ontology.origintrail.io/dkg/1.0#privateMerkleRoot";

/// Predicate marking a resource as representing private data
pub const REPRESENTS_PRIVATE_RESOURCE: &str =
    "https://ontology.origintrail.io/dkg/1.0#representsPrivateResource";
