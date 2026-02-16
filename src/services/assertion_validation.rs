use std::sync::Arc;

use dkg_domain::{Assertion, ParsedUal, Visibility};

use crate::managers::{
    blockchain::BlockchainManager,
    triple_store::{
        compare_js_default_string_order, group_triples_by_subject,
        query::{predicates::PRIVATE_MERKLE_ROOT, subjects::PRIVATE_HASH_SUBJECT_PREFIX},
        rdf::extract_quoted_string,
    },
};

/// Service for validating assertion responses.
///
/// Validates that:
/// 1. Public assertion merkle root matches on-chain value
/// 2. Private assertion merkle root (if present) matches the value in public triples
pub(crate) struct AssertionValidationService {
    blockchain_manager: Arc<BlockchainManager>,
}

impl AssertionValidationService {
    pub(crate) fn new(blockchain_manager: Arc<BlockchainManager>) -> Self {
        Self { blockchain_manager }
    }

    fn should_skip_validation(
        assertion: &Assertion,
        parsed_ual: &ParsedUal,
        visibility: Visibility,
    ) -> bool {
        // For single KA, skip validation (like JS)
        if parsed_ual.knowledge_asset_id.is_some() {
            return true;
        }

        // If only private content is requested and there's no public, skip validation
        assertion.public.is_empty()
            && assertion.private.is_some()
            && visibility == Visibility::Private
    }

    /// Validate a response with assertions (local or network).
    ///
    /// Follows the same logic as JS validateResponse:
    /// - For single KA: skip validation (return true)
    /// - For collection: validate public merkle root against on-chain, validate private merkle root
    ///   if present
    ///
    /// Returns true if the response is valid, false otherwise.
    pub(crate) async fn validate_response(
        &self,
        assertion: &Assertion,
        parsed_ual: &ParsedUal,
        visibility: Visibility,
    ) -> bool {
        if Self::should_skip_validation(assertion, parsed_ual, visibility) {
            return true;
        }

        // Validate public assertion if present
        if !assertion.public.is_empty() {
            match self
                .validate_public_assertion(&assertion.public, parsed_ual)
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
        if let Some(private) = &assertion.private
            && !private.is_empty()
        {
            match self.validate_private_assertion(&assertion.public, private) {
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

    /// Validate a response using a pre-fetched merkle root.
    ///
    /// This is more efficient than `validate_response` when validating multiple KCs
    /// because it doesn't need to make individual RPC calls for each merkle root.
    pub(crate) fn validate_response_with_root(
        &self,
        assertion: &Assertion,
        parsed_ual: &ParsedUal,
        visibility: Visibility,
        merkle_root: Option<&str>,
    ) -> bool {
        if Self::should_skip_validation(assertion, parsed_ual, visibility) {
            return true;
        }

        // Validate public assertion if present
        if !assertion.public.is_empty() {
            let Some(on_chain_root) = merkle_root else {
                tracing::debug!("No merkle root provided for validation");
                return false;
            };

            if !self.validate_public_assertion_with_root(
                &assertion.public,
                on_chain_root,
                parsed_ual,
            ) {
                tracing::debug!("Public assertion validation failed");
                return false;
            }
        }

        // Validate private assertion if present
        if let Some(private) = &assertion.private
            && !private.is_empty()
        {
            match self.validate_private_assertion(&assertion.public, private) {
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

    /// Validate public assertion against a pre-fetched on-chain merkle root.
    fn validate_public_assertion_with_root(
        &self,
        public_triples: &[String],
        on_chain_root: &str,
        parsed_ual: &ParsedUal,
    ) -> bool {
        let calculated_root = match Self::calculate_public_merkle_root(public_triples) {
            Ok(root) => root,
            Err(e) => {
                tracing::debug!(error = %e, "Failed to calculate public merkle root");
                return false;
            }
        };

        if calculated_root != on_chain_root {
            let assertion_public = serialize_triples_for_log(public_triples);
            tracing::debug!(
                kc_ual = %parsed_ual.knowledge_collection_ual(),
                calculated = %calculated_root,
                on_chain = %on_chain_root,
                public_count = public_triples.len(),
                assertion_public = %assertion_public,
                "Merkle root mismatch"
            );
            return false;
        }

        true
    }

    /// Calculate the merkle root for public triples.
    fn calculate_public_merkle_root(public_triples: &[String]) -> Result<String, String> {
        // Network payloads can contain multiple triples per string entry.
        // Normalize to one triple per line before grouping/sorting so hashing
        // matches JS behavior and on-chain roots.
        let normalized_public = Self::normalize_public_triples(public_triples);

        // Separate private-hash triples from regular public triples
        let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);

        let mut filtered_public: Vec<String> = Vec::new();
        let mut private_hash_triples: Vec<String> = Vec::new();

        for triple in normalized_public {
            if triple.starts_with(&private_hash_prefix) {
                private_hash_triples.push(triple);
            } else {
                filtered_public.push(triple);
            }
        }

        // Group by subject, then append private-hash groups
        let mut grouped = group_triples_by_subject(&filtered_public)?;
        grouped.extend(group_triples_by_subject(&private_hash_triples)?);

        // Sort each group and flatten
        let sorted_flat: Vec<String> = grouped
            .iter()
            .flat_map(|group| {
                let mut sorted_group: Vec<&str> = group.iter().map(String::as_str).collect();
                sorted_group.sort_by(|a, b| compare_js_default_string_order(a, b));
                sorted_group.into_iter().map(String::from)
            })
            .collect();

        // Calculate merkle root
        Ok(dkg_domain::calculate_merkle_root(&sorted_flat))
    }

    fn normalize_public_triples(public_triples: &[String]) -> Vec<String> {
        public_triples
            .iter()
            .flat_map(|entry| entry.lines())
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect()
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
        let calculated_root = Self::calculate_public_merkle_root(public_triples)
            .map_err(|e| format!("Failed to calculate public merkle root: {}", e))?;

        // Get on-chain merkle root
        let on_chain_root = self
            .blockchain_manager
            .get_knowledge_collection_merkle_root(
                &parsed_ual.blockchain,
                parsed_ual.contract,
                parsed_ual.knowledge_collection_id,
            )
            .await
            .map_err(|e| format!("Failed to get on-chain merkle root: {}", e))?
            .ok_or_else(|| "Knowledge collection not found on-chain".to_string())?;

        if calculated_root != on_chain_root {
            let assertion_public = serialize_triples_for_log(public_triples);
            tracing::debug!(
                kc_ual = %parsed_ual.knowledge_collection_ual(),
                calculated = %calculated_root,
                on_chain = %on_chain_root,
                public_count = public_triples.len(),
                assertion_public = %assertion_public,
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
        sorted_private.sort_by(|a, b| compare_js_default_string_order(a, b));

        let calculated_root = dkg_domain::calculate_merkle_root(&sorted_private);

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
    // Extract the quoted object literal (handles typed literals too).
    let value = extract_quoted_string(triple)?;

    // Should start with 0x for a merkle root
    if value.starts_with("0x") {
        Some(value)
    } else {
        None
    }
}

fn serialize_triples_for_log(triples: &[String]) -> String {
    serde_json::to_string(triples).unwrap_or_else(|e| format!("\"<serialization_error:{}>\"", e))
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
    fn test_extract_private_merkle_root_typed_literal() {
        let triple = r#"<subject> <https://ontology.origintrail.io/dkg/1.0#privateMerkleRoot> "0xabc"^^<http://www.w3.org/2001/XMLSchema#string> ."#;
        let root = extract_private_merkle_root(triple);
        assert_eq!(root, Some("0xabc".to_string()));
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

    #[test]
    fn test_calculate_public_merkle_root_matches_known_paranet_kc() {
        let triples = sample_kc_4210492_public_triples();
        let root = AssertionValidationService::calculate_public_merkle_root(&triples)
            .expect("Expected merkle root calculation to succeed");
        assert_eq!(
            root,
            "0xf0c149201a9eb5a3b28bfa18e804ba792776ec5ec18225a051a010b18253c97e"
        );
    }

    #[test]
    fn test_calculate_public_merkle_root_matches_known_paranet_kc_chunked_payload() {
        let triples = sample_kc_4210492_public_triples();

        // Mimic ACK payloads where each entry may contain multiple complete lines.
        let chunked: Vec<String> = triples.chunks(4).map(|chunk| chunk.join("\n")).collect();

        let root = AssertionValidationService::calculate_public_merkle_root(&chunked)
            .expect("Expected merkle root calculation to succeed");
        assert_eq!(
            root,
            "0xf0c149201a9eb5a3b28bfa18e804ba792776ec5ec18225a051a010b18253c97e"
        );
    }

    #[test]
    fn test_private_sort_uses_js_default_utf16_order() {
        let mut triples = ["a\u{E000}", "a\u{10000}"]
            .into_iter()
            .map(String::from)
            .collect::<Vec<String>>();
        triples.sort_by(|a, b| compare_js_default_string_order(a, b));
        assert_eq!(
            triples,
            vec!["a\u{10000}".to_string(), "a\u{E000}".to_string()]
        );
    }

    fn sample_kc_4210492_public_triples() -> Vec<String> {
        vec![
            r#"<uuid:4b2ab12a-8011-4bd1-8189-6c2da145ed66> <http://schema.org/about> <uuid:DzyraSwarm> ."#.to_string(),
            r#"<uuid:4b2ab12a-8011-4bd1-8189-6c2da145ed66> <http://schema.org/articleBody> "@DzyraSwarm Appreciate you stopping by and sharing that." ."#.to_string(),
            r#"<uuid:4b2ab12a-8011-4bd1-8189-6c2da145ed66> <http://schema.org/author> <uuid:ross:power> ."#.to_string(),
            r#"<uuid:4b2ab12a-8011-4bd1-8189-6c2da145ed66> <http://schema.org/dateCreated> "2024-01-03T00:47:00Z" ."#.to_string(),
            r#"<uuid:4b2ab12a-8011-4bd1-8189-6c2da145ed66> <http://schema.org/headline> "Ross Power appreciates D.Zyra's input." ."#.to_string(),
            r#"<uuid:4b2ab12a-8011-4bd1-8189-6c2da145ed66> <http://schema.org/keywords> <uuid:appreciation> ."#.to_string(),
            r#"<uuid:4b2ab12a-8011-4bd1-8189-6c2da145ed66> <http://schema.org/keywords> <uuid:sharing> ."#.to_string(),
            r#"<uuid:4b2ab12a-8011-4bd1-8189-6c2da145ed66> <http://schema.org/mentions> <uuid:d6f78b11-d1eb-466f-83b8-f18bd3622fc7> ."#.to_string(),
            r#"<uuid:4b2ab12a-8011-4bd1-8189-6c2da145ed66> <http://schema.org/url> <https://twitter.com/rosspower/status/1951288768821928391> ."#.to_string(),
            r#"<uuid:4b2ab12a-8011-4bd1-8189-6c2da145ed66> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/SocialMediaPosting> ."#.to_string(),
            r#"<uuid:DzyraSwarm> <http://schema.org/name> "D.Zyra's Input" ."#.to_string(),
            r#"<uuid:DzyraSwarm> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Thing> ."#.to_string(),
            r#"<uuid:appreciation> <http://schema.org/name> "Appreciation" ."#.to_string(),
            r#"<uuid:appreciation> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Text> ."#.to_string(),
            r#"<uuid:d6f78b11-d1eb-466f-83b8-f18bd3622fc7> <http://schema.org/identifier> "@DzyraSwarm" ."#.to_string(),
            r#"<uuid:d6f78b11-d1eb-466f-83b8-f18bd3622fc7> <http://schema.org/name> "D.Zyra" ."#.to_string(),
            r#"<uuid:d6f78b11-d1eb-466f-83b8-f18bd3622fc7> <http://schema.org/url> <https://twitter.com/DzyraSwarm> ."#.to_string(),
            r#"<uuid:d6f78b11-d1eb-466f-83b8-f18bd3622fc7> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> ."#.to_string(),
            r#"<uuid:ross:power> <http://schema.org/identifier> "@rosspower" ."#.to_string(),
            r#"<uuid:ross:power> <http://schema.org/name> "Ross Power" ."#.to_string(),
            r#"<uuid:ross:power> <http://schema.org/url> <https://twitter.com/rosspower> ."#.to_string(),
            r#"<uuid:ross:power> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> ."#.to_string(),
            r#"<uuid:sharing> <http://schema.org/name> "Sharing" ."#.to_string(),
            r#"<uuid:sharing> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Text> ."#.to_string(),
        ]
    }
}
