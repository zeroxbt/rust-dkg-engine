use serde::{Deserialize, Serialize};
use validator::ValidationError;
use validator_derive::Validate;

/// Validates that the public triples vector is non-empty.
fn validate_public_non_empty(public: &[String]) -> Result<(), ValidationError> {
    if public.is_empty() {
        let mut error = ValidationError::new("public_non_empty");
        error.message = Some("public triples must contain at least one triple".into());
        return Err(error);
    }
    Ok(())
}

/// An assertion containing public and optional private triples.
///
/// This is the core data structure for knowledge assets in the DKG,
/// containing the N-Quads/N-Triples representation of RDF data.
#[derive(Debug, Serialize, Deserialize, Clone, Default, Validate)]
pub(crate) struct Assertion {
    /// Public triples (required, at least one triple)
    #[validate(custom(function = "validate_public_non_empty"))]
    pub public: Vec<String>,
    /// Private triples (optional)
    pub private: Option<Vec<String>>,
}

impl Assertion {
    /// Create a new assertion with public and optional private triples.
    pub(crate) fn new(public: Vec<String>, private: Option<Vec<String>>) -> Self {
        Self { public, private }
    }

    /// Create a new assertion with only public triples.
    pub(crate) fn public_only(public: Vec<String>) -> Self {
        Self {
            public,
            private: None,
        }
    }

    /// Create a new assertion with both public and private triples.
    pub(crate) fn with_private(public: Vec<String>, private: Vec<String>) -> Self {
        Self {
            public,
            private: Some(private),
        }
    }

    /// Check if the assertion has any data.
    pub(crate) fn has_data(&self) -> bool {
        !self.public.is_empty() || self.private.as_ref().is_some_and(|p| !p.is_empty())
    }

    /// Check if the public triples are empty.
    pub(crate) fn is_public_empty(&self) -> bool {
        self.public.is_empty()
    }

    /// Get the total number of triples in the assertion.
    pub(crate) fn total_triples(&self) -> usize {
        self.public.len() + self.private.as_ref().map_or(0, |p| p.len())
    }
}
