//! Named graph constants matching the DKG specification.
//!
//! These are the standard named graphs used by the OriginTrail DKG
//! for organizing RDF data.

/// Metadata graph storing KC/KA metadata
pub const METADATA: &str = "metadata:graph";

/// Current graph tracking active named graphs
pub const CURRENT: &str = "current:graph";
