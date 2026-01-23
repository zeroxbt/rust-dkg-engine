use std::sync::Arc;

use crate::{
    managers::{
        blockchain::BlockchainManager,
        triple_store::{
            Visibility, group_nquads_by_subject,
            query::{predicates::PRIVATE_MERKLE_ROOT, subjects::PRIVATE_HASH_SUBJECT_PREFIX},
        },
    },
    utils::{ual::ParsedUal, validation},
};

/// Service for validating get operation responses.
///
/// Validates that:
/// 1. Public assertion merkle root matches on-chain value
/// 2. Private assertion merkle root (if present) matches the value in public triples
pub(crate) struct GetValidationService {
    blockchain_manager: Arc<BlockchainManager>,
}

impl GetValidationService {
    pub(crate) fn new(blockchain_manager: Arc<BlockchainManager>) -> Self {
        Self { blockchain_manager }
    }

    /// Validate a get response (local or network).
    ///
    /// Follows the same logic as JS validateResponse:
    /// - For single KA: skip validation (return true)
    /// - For collection: validate public merkle root against on-chain, validate private merkle root
    ///   if present
    ///
    /// Returns true if the response is valid, false otherwise.
    pub(crate) async fn validate_response(
        &self,
        public_triples: &[String],
        private_triples: Option<&[String]>,
        parsed_ual: &ParsedUal,
        visibility: Visibility,
    ) -> bool {
        // For single KA, skip validation (like JS)
        if parsed_ual.knowledge_asset_id.is_some() {
            return true;
        }

        // If only private content is requested and there's no public, skip validation
        if public_triples.is_empty()
            && private_triples.is_some()
            && visibility == Visibility::Private
        {
            return true;
        }

        // Validate public assertion if present
        if !public_triples.is_empty() {
            match self
                .validate_public_assertion(public_triples, parsed_ual)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    tracing::debug!("Public assertion validation failed");
                    return false;
                }
                Err(e) => {
                    tracing::debug!(error = %e, "Error validating public assertion");
                    return false;
                }
            }
        }

        // Validate private assertion if present
        if let Some(private) = private_triples
            && !private.is_empty()
        {
            match self.validate_private_assertion(public_triples, private) {
                Ok(true) => {}
                Ok(false) => {
                    tracing::debug!("Private assertion validation failed");
                    return false;
                }
                Err(e) => {
                    tracing::debug!(error = %e, "Error validating private assertion");
                    return false;
                }
            }
        }

        true
    }

    /// Validate public assertion against on-chain merkle root.
    ///
    /// This:
    /// 1. Separates private-hash triples from regular public triples
    /// 2. Groups triples by subject
    /// 3. Sorts each group and flattens
    /// 4. Calculates merkle root
    /// 5. Compares with on-chain value
    async fn validate_public_assertion(
        &self,
        public_triples: &[String],
        parsed_ual: &ParsedUal,
    ) -> Result<bool, String> {
        // Separate private-hash triples from regular public triples
        let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);

        let mut filtered_public: Vec<&str> = Vec::new();
        let mut private_hash_triples: Vec<&str> = Vec::new();

        for triple in public_triples {
            if triple.starts_with(&private_hash_prefix) {
                private_hash_triples.push(triple);
            } else {
                filtered_public.push(triple);
            }
        }

        // Group by subject, then append private-hash groups
        let mut grouped = group_nquads_by_subject(&filtered_public);
        grouped.extend(group_nquads_by_subject(&private_hash_triples));

        // Sort each group and flatten
        let sorted_flat: Vec<String> = grouped
            .iter()
            .flat_map(|group| {
                let mut sorted_group: Vec<&str> = group.to_vec();
                sorted_group.sort();
                sorted_group.into_iter().map(String::from)
            })
            .collect();

        // Calculate merkle root
        let calculated_root = validation::calculate_merkle_root(&sorted_flat);

        // Get on-chain merkle root
        let on_chain_root = self
            .blockchain_manager
            .get_knowledge_collection_merkle_root(
                &parsed_ual.blockchain,
                parsed_ual.knowledge_collection_id,
            )
            .await
            .map_err(|e| format!("Failed to get on-chain merkle root: {}", e))?
            .ok_or_else(|| "Knowledge collection not found on-chain".to_string())?;

        if calculated_root != on_chain_root {
            tracing::debug!(
                calculated = %calculated_root,
                on_chain = %on_chain_root,
                "Merkle root mismatch"
            );
            return Ok(false);
        }

        Ok(true)
    }

    /// Validate private assertion merkle root.
    ///
    /// This:
    /// 1. Finds the privateMerkleRoot predicate in public triples
    /// 2. Extracts the expected merkle root
    /// 3. Sorts private triples and calculates merkle root
    /// 4. Compares the values
    fn validate_private_assertion(
        &self,
        public_triples: &[String],
        private_triples: &[String],
    ) -> Result<bool, String> {
        // Find the triple with privateMerkleRoot predicate
        let private_root_triple = public_triples
            .iter()
            .find(|triple| triple.contains(PRIVATE_MERKLE_ROOT));

        let Some(root_triple) = private_root_triple else {
            // No private merkle root in public triples - can't validate
            // This is ok, private data might just not have a root triple
            return Ok(true);
        };

        // Extract the merkle root value from the triple
        // Format: <subject> <predicate> "0x..." .
        let expected_root = extract_private_merkle_root(root_triple)
            .ok_or_else(|| "Failed to extract private merkle root from triple".to_string())?;

        // Sort private triples and calculate merkle root
        let mut sorted_private: Vec<String> = private_triples.to_vec();
        sorted_private.sort();

        let calculated_root = validation::calculate_merkle_root(&sorted_private);

        if calculated_root != expected_root {
            tracing::debug!(
                calculated = %calculated_root,
                expected = %expected_root,
                "Private merkle root mismatch"
            );
            return Ok(false);
        }

        Ok(true)
    }
}

/// Extract the merkle root value from a privateMerkleRoot triple.
///
/// Expected format: `<subject> <https://ontology.origintrail.io/dkg/1.0#privateMerkleRoot> "0x..." .`
fn extract_private_merkle_root(triple: &str) -> Option<String> {
    // The object is typically the third space-separated part, quoted
    // Format: <s> <p> "value" .
    let parts: Vec<&str> = triple.split(' ').collect();
    if parts.len() < 3 {
        return None;
    }

    // Object is parts[2], remove quotes
    let object = parts[2];
    let value = object.trim_matches('"');

    // Should start with 0x for a merkle root
    if value.starts_with("0x") {
        Some(value.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Check if public assertion contains privateMerkleRoot predicate.
    ///
    /// Used to determine if private triples are expected for permissioned paranets.
    pub(crate) fn assertion_has_private_merkle_root(public_triples: &[String]) -> bool {
        public_triples
            .iter()
            .any(|triple| triple.contains(PRIVATE_MERKLE_ROOT))
    }

    #[test]
    fn test_extract_private_merkle_root() {
        let triple = r#"<uuid:1e91a527-a3ef-430e-819e-64710ab0f797> <https://ontology.origintrail.io/dkg/1.0#privateMerkleRoot> "0xaac2a420672a1eb77506c544ff01beed2be58c0ee3576fe037c846f97481cefd" ."#;
        let root = extract_private_merkle_root(triple);
        assert_eq!(
            root,
            Some("0xaac2a420672a1eb77506c544ff01beed2be58c0ee3576fe037c846f97481cefd".to_string())
        );
    }

    #[test]
    fn test_extract_private_merkle_root_no_match() {
        let triple = r#"<subject> <predicate> "not-a-merkle-root" ."#;
        let root = extract_private_merkle_root(triple);
        assert_eq!(root, None);
    }

    #[test]
    fn test_assertion_has_private_merkle_root() {
        let triples = vec![
            r#"<subject> <http://schema.org/name> "Test" ."#.to_string(),
            r#"<uuid:123> <https://ontology.origintrail.io/dkg/1.0#privateMerkleRoot> "0xabc" ."#
                .to_string(),
        ];
        assert!(assertion_has_private_merkle_root(&triples));

        let triples_no_private = vec![r#"<subject> <http://schema.org/name> "Test" ."#.to_string()];
        assert!(!assertion_has_private_merkle_root(&triples_no_private));
    }
}
