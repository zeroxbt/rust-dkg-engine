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
/// containing RDF lines (N-Triples/N-Quads) for public and private data.
#[derive(Debug, Serialize, Deserialize, Clone, Default, Validate)]
pub struct Assertion {
    /// Public triples (required, at least one triple)
    #[validate(custom(function = "validate_public_non_empty"))]
    pub public: Vec<String>,
    /// Private triples (optional)
    pub private: Option<Vec<String>>,
}

impl Assertion {
    /// Create a new assertion with public and optional private triples.
    pub fn new(public: Vec<String>, private: Option<Vec<String>>) -> Self {
        Self { public, private }
    }

    /// Check if the assertion has any data.
    pub fn has_data(&self) -> bool {
        !self.public.is_empty() || self.private.as_ref().is_some_and(|p| !p.is_empty())
    }
}
