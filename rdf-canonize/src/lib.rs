//! # URDNA2015 Canonicalization Algorithm
//!
//! This module defines the public API for the URDNA2015 canonicalization algorithm.
//! It provides functionality to canonize JSON-LD documents into normalized N-Quads format.
//! This is particularly useful in RDF data processing and digital signature applications
//! where consistent serialization of RDF datasets is required.

mod error;
mod identifier_issuer;
mod message_digest;
mod n_quad;
mod permuter;
mod urdna2015;

use error::URDNAError;
use n_quad::NQuads;
use urdna2015::URDNA2015;

/// Canonizes a JSON-LD document into a normalized N-Quads format string.
///
/// # Arguments
/// * `input` - A string slice that holds the JSON-LD document.
///
/// # Returns
/// This function returns a `Result<String, URDNAError>`. On success, it provides the
/// canonicalized N-Quads string. On failure, it returns an `URDNAError` enum indicating the type of
/// error.
///
/// # Errors
/// This function can return `Parsing` if the input document cannot be parsed correctly,
/// `Serializing` during serialization of quad terms or
/// `Hashing` during processing of hashing operations
///
/// # Examples
/// ```
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let jsonld = r#"{ "@context": { "@vocab": "http://example.org/" }, "example": "data" }"#;
/// let nquads = urdna2015::canonize(jsonld).await?;
/// println!("Canonicalized N-Quads: {}", nquads);
/// # Ok(())
/// # }
/// ```
pub async fn canonize(input: &str) -> Result<String, URDNAError> {
    let mut algo = URDNA2015::new();
    let n_quads = NQuads::parse(input)?;
    algo.main(n_quads).await
}
