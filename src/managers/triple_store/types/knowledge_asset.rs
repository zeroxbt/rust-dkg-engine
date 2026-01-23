//! Knowledge Asset type for triple store operations.

/// A Knowledge Asset with its associated triples.
///
/// This represents a single knowledge asset within a knowledge collection,
/// with its UAL and the public/private triples that belong to it.
#[derive(Debug, Clone)]
pub(crate) struct KnowledgeAsset {
    /// The UAL (Universal Asset Locator) for this knowledge asset
    pub ual: String,
    /// Public triples (N-Quads format)
    pub public_triples: Vec<String>,
    /// Private triples (N-Quads format), if any
    pub private_triples: Option<Vec<String>>,
}

impl KnowledgeAsset {
    /// Create a new KnowledgeAsset with only public triples.
    pub(crate) fn new(ual: String, public_triples: Vec<String>) -> Self {
        Self {
            ual,
            public_triples,
            private_triples: None,
        }
    }

    /// Create a new KnowledgeAsset with both public and private triples.
    pub(crate) fn with_private(
        ual: String,
        public_triples: Vec<String>,
        private_triples: Vec<String>,
    ) -> Self {
        Self {
            ual,
            public_triples,
            private_triples: Some(private_triples),
        }
    }

    /// Returns the UAL of this knowledge asset.
    pub(crate) fn ual(&self) -> &str {
        &self.ual
    }

    /// Returns the public triples.
    pub(crate) fn public_triples(&self) -> &[String] {
        &self.public_triples
    }

    /// Returns the private triples, if any.
    pub(crate) fn private_triples(&self) -> Option<&[String]> {
        self.private_triples.as_deref()
    }

    /// Returns true if this knowledge asset has private triples.
    pub(crate) fn has_private(&self) -> bool {
        self.private_triples.as_ref().is_some_and(|t| !t.is_empty())
    }

    /// Add private triples to this knowledge asset.
    pub(crate) fn set_private_triples(&mut self, triples: Vec<String>) {
        self.private_triples = Some(triples);
    }
}
