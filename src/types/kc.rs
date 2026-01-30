/// Metadata for a knowledge collection
#[derive(Debug, Clone)]
pub(crate) struct KnowledgeCollectionMetadata {
    publisher_address: String,
    block_number: u64,
    transaction_hash: String,
    block_timestamp: u64,
}

impl KnowledgeCollectionMetadata {
    pub(crate) fn new(
        publisher_address: String,
        block_number: u64,
        transaction_hash: String,
        block_timestamp: u64,
    ) -> Self {
        Self {
            publisher_address,
            block_number,
            transaction_hash,
            block_timestamp,
        }
    }

    pub(crate) fn publisher_address(&self) -> &str {
        &self.publisher_address
    }

    pub(crate) fn block_number(&self) -> u64 {
        self.block_number
    }

    pub(crate) fn transaction_hash(&self) -> &str {
        &self.transaction_hash
    }

    pub(crate) fn block_timestamp(&self) -> u64 {
        self.block_timestamp
    }
}

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

    /// Add private triples to this knowledge asset.
    pub(crate) fn set_private_triples(&mut self, triples: Vec<String>) {
        self.private_triples = Some(triples);
    }
}
