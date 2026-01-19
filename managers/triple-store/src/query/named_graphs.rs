//! Named graph constants matching the DKG specification.
//!
//! These are the standard named graphs used by the OriginTrail DKG
//! for organizing RDF data.

/// Unified graph containing all knowledge with UAL annotations
pub const UNIFIED: &str = "unified:graph";

/// Historical unified graph for versioned data
pub const HISTORICAL_UNIFIED: &str = "historical-unified:graph";

/// Metadata graph storing KC/KA metadata
pub const METADATA: &str = "metadata:graph";

/// Current graph tracking active named graphs
pub const CURRENT: &str = "current:graph";
