use std::{collections::HashMap, sync::Arc};

use crate::{
    managers::triple_store::{
        Assertion, KnowledgeAsset, KnowledgeCollectionMetadata, TokenIds, TripleStoreManager,
        Visibility, error::TripleStoreError, extract_subject, group_nquads_by_subject,
        query::subjects::PRIVATE_HASH_SUBJECT_PREFIX,
    },
    utils::ual::ParsedUal,
};

/// Result of querying assertion data from the triple store.
#[derive(Debug, Clone)]
pub(crate) struct AssertionQueryResult {
    pub assertion: Assertion,
    pub metadata: Option<Vec<String>>,
}

/// Service for querying assertion data from the triple store.
///
/// This service provides a unified interface for querying knowledge assets
/// and collections, used by both the get sender (local query) and receiver
/// (handling remote requests).
pub(crate) struct TripleStoreService {
    triple_store_manager: Arc<TripleStoreManager>,
}

impl TripleStoreService {
    pub(crate) fn new(triple_store_manager: Arc<TripleStoreManager>) -> Self {
        Self {
            triple_store_manager,
        }
    }

    /// Query assertion data from the triple store.
    ///
    /// Follows the same logic as JS tripleStoreService.getAssertion:
    /// - For single KA: query named graph directly
    /// - For collection: check if first/last KA exists, then query all named graphs
    ///
    /// Returns the query result with public, private, and metadata triples,
    /// or None if not found or an error occurred.
    pub(crate) async fn query_assertion(
        &self,
        parsed_ual: &ParsedUal,
        token_ids: &TokenIds,
        visibility: Visibility,
        include_metadata: bool,
    ) -> Option<AssertionQueryResult> {
        let kc_ual = parsed_ual.knowledge_collection_ual();

        let assertion = if parsed_ual.knowledge_asset_id.is_some() {
            self.query_single_asset(&parsed_ual.to_ual_string(), visibility)
                .await?
        } else {
            self.query_collection(&kc_ual, token_ids, visibility)
                .await?
        };

        // Check if we found any data
        if !assertion.has_data() {
            return None;
        }

        // Query metadata if requested
        let metadata = if include_metadata {
            self.query_metadata(&kc_ual).await
        } else {
            None
        };

        Some(AssertionQueryResult {
            assertion,
            metadata,
        })
    }

    /// Query a single knowledge asset.
    async fn query_single_asset(&self, ka_ual: &str, visibility: Visibility) -> Option<Assertion> {
        // Query public triples if requested
        let public = if visibility == Visibility::Public || visibility == Visibility::All {
            match self
                .triple_store_manager
                .get_knowledge_asset_named_graph(ka_ual, Visibility::Public)
                .await
            {
                Ok(triples) => triples,
                Err(e) => {
                    tracing::debug!(error = %e, "Failed to query public triples");
                    return None;
                }
            }
        } else {
            Vec::new()
        };

        // Query private triples if requested
        let private = if visibility == Visibility::Private || visibility == Visibility::All {
            match self
                .triple_store_manager
                .get_knowledge_asset_named_graph(ka_ual, Visibility::Private)
                .await
            {
                Ok(triples) if !triples.is_empty() => Some(triples),
                _ => None,
            }
        } else {
            None
        };

        Some(Assertion::new(public, private))
    }

    /// Query a knowledge collection.
    async fn query_collection(
        &self,
        kc_ual: &str,
        token_ids: &TokenIds,
        visibility: Visibility,
    ) -> Option<Assertion> {
        // First check if first and last KA exist (like JS)
        let first_ka_ual = format!("{}/{}/public", kc_ual, token_ids.start_token_id());
        let last_ka_ual = format!("{}/{}/public", kc_ual, token_ids.end_token_id());

        let (first_exists, last_exists) = tokio::join!(
            self.triple_store_manager
                .knowledge_asset_exists(&first_ka_ual),
            self.triple_store_manager
                .knowledge_asset_exists(&last_ka_ual)
        );

        let first_exists = first_exists.unwrap_or(false);
        let last_exists = last_exists.unwrap_or(false);

        if !first_exists || !last_exists {
            tracing::debug!(
                first_exists = first_exists,
                last_exists = last_exists,
                "Knowledge collection does not exist locally"
            );
            return None;
        }

        // Query public triples if requested
        let public = if visibility == Visibility::Public || visibility == Visibility::All {
            match self
                .triple_store_manager
                .get_knowledge_collection_named_graphs(
                    kc_ual,
                    token_ids.start_token_id(),
                    token_ids.end_token_id(),
                    token_ids.burned(),
                    Visibility::Public,
                )
                .await
            {
                Ok(triples) => triples,
                Err(e) => {
                    tracing::debug!(error = %e, "Failed to query public collection triples");
                    return None;
                }
            }
        } else {
            Vec::new()
        };

        // Query private triples if requested
        let private = if visibility == Visibility::Private || visibility == Visibility::All {
            match self
                .triple_store_manager
                .get_knowledge_collection_named_graphs(
                    kc_ual,
                    token_ids.start_token_id(),
                    token_ids.end_token_id(),
                    token_ids.burned(),
                    Visibility::Private,
                )
                .await
            {
                Ok(triples) if !triples.is_empty() => Some(triples),
                _ => None,
            }
        } else {
            None
        };

        Some(Assertion::new(public, private))
    }

    /// Query metadata for a knowledge collection.
    async fn query_metadata(&self, kc_ual: &str) -> Option<Vec<String>> {
        match self.triple_store_manager.get_metadata(kc_ual).await {
            Ok(metadata_nquads) => {
                let metadata: Vec<String> = metadata_nquads
                    .lines()
                    .filter(|line| !line.trim().is_empty())
                    .map(String::from)
                    .collect();
                if metadata.is_empty() {
                    None
                } else {
                    Some(metadata)
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "Failed to query metadata from triple store");
                None
            }
        }
    }

    /// Query assertion data for multiple UALs in batch.
    ///
    /// Iterates over the provided UALs and queries each one, collecting results
    /// into a HashMap. Only UALs that have data are included in the result.
    ///
    /// This method is used by the batch get protocol to efficiently query
    /// multiple assets in a single operation.
    pub(crate) async fn query_assertions_batch(
        &self,
        uals_with_token_ids: &[(ParsedUal, TokenIds)],
        visibility: Visibility,
        include_metadata: bool,
    ) -> HashMap<String, AssertionQueryResult> {
        let mut results = HashMap::new();

        for (parsed_ual, token_ids) in uals_with_token_ids {
            let ual_string = parsed_ual.to_ual_string();

            if let Some(result) = self
                .query_assertion(parsed_ual, token_ids, visibility, include_metadata)
                .await
                && result.assertion.has_data()
            {
                results.insert(ual_string, result);
            }
        }

        results
    }

    /// Insert a knowledge collection into the triple store.
    ///
    /// This function:
    /// 1. Separates public triples into regular and private-hash triples
    /// 2. Groups triples by subject to form knowledge assets
    /// 3. Matches private triples to their corresponding public knowledge assets
    /// 4. Delegates RDF serialization and SPARQL building to TripleStoreManager
    ///
    /// Returns the total number of triples inserted, or an error.
    pub(crate) async fn insert_knowledge_collection(
        &self,
        knowledge_collection_ual: &str,
        dataset: &Assertion,
        metadata: &Option<KnowledgeCollectionMetadata>,
    ) -> Result<usize, TripleStoreError> {
        // Build knowledge assets from the dataset
        let knowledge_assets = Self::build_knowledge_assets(knowledge_collection_ual, dataset);

        // Delegate to the triple store manager for RDF serialization and insertion
        self.triple_store_manager
            .insert_knowledge_collection(knowledge_collection_ual, &knowledge_assets, metadata)
            .await
    }

    /// Check if a knowledge collection exists locally in the triple store.
    ///
    /// Checks if both the first and last knowledge assets exist, which indicates
    /// the entire collection is present locally.
    pub(crate) async fn knowledge_collection_exists(
        &self,
        kc_ual: &str,
        start_token_id: u64,
        end_token_id: u64,
    ) -> bool {
        let first_ka_ual = format!("{}/{}/public", kc_ual, start_token_id);
        let last_ka_ual = format!("{}/{}/public", kc_ual, end_token_id);

        let (first_exists, last_exists) = tokio::join!(
            self.triple_store_manager
                .knowledge_asset_exists(&first_ka_ual),
            self.triple_store_manager
                .knowledge_asset_exists(&last_ka_ual)
        );

        first_exists.unwrap_or(false) && last_exists.unwrap_or(false)
    }

    /// Check if a knowledge collection exists by UAL only (no token range needed).
    ///
    /// This is a fast existence check that queries the metadata graph.
    /// Useful when you don't have the token range but need to check if a KC is already synced.
    pub(crate) async fn knowledge_collection_exists_by_ual(&self, kc_ual: &str) -> bool {
        self.triple_store_manager
            .knowledge_collection_exists_by_ual(kc_ual)
            .await
            .unwrap_or(false)
    }

    /// Build knowledge assets from a dataset.
    ///
    /// This contains the DKG business logic:
    /// - Separating public triples from private-hash triples
    /// - Grouping triples by subject
    /// - Generating UALs for each knowledge asset
    /// - Matching private triples to public knowledge assets
    fn build_knowledge_assets(
        knowledge_collection_ual: &str,
        dataset: &Assertion,
    ) -> Vec<KnowledgeAsset> {
        let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);

        // Separate public triples: regular public vs private-hash triples
        let mut filtered_public: Vec<&str> = Vec::new();
        let mut private_hash_triples: Vec<&str> = Vec::new();

        for triple in &dataset.public {
            if triple.starts_with(&private_hash_prefix) {
                private_hash_triples.push(triple);
            } else {
                filtered_public.push(triple);
            }
        }

        // Group public triples by subject, then append private-hash groups
        let mut public_ka_triples_grouped = group_nquads_by_subject(&filtered_public);
        public_ka_triples_grouped.extend(group_nquads_by_subject(&private_hash_triples));

        // Generate UALs for each public knowledge asset: {kc_ual}/1, {kc_ual}/2, ...
        let public_ka_uals: Vec<String> = (0..public_ka_triples_grouped.len())
            .map(|i| format!("{}/{}", knowledge_collection_ual, i + 1))
            .collect();

        // Create knowledge assets with public triples
        let mut knowledge_assets: Vec<KnowledgeAsset> = public_ka_triples_grouped
            .iter()
            .zip(public_ka_uals.iter())
            .map(|(triples, ual)| {
                KnowledgeAsset::new(ual.clone(), triples.iter().map(|s| s.to_string()).collect())
            })
            .collect();

        // Match and attach private triples if present
        if let Some(private_triples) = &dataset.private
            && !private_triples.is_empty()
        {
            let private_refs: Vec<&str> = private_triples.iter().map(|s| s.as_str()).collect();
            let private_ka_triples_grouped = group_nquads_by_subject(&private_refs);

            // Build a map from public subject -> index for matching
            let public_subject_map: HashMap<&str, usize> = public_ka_triples_grouped
                .iter()
                .enumerate()
                .filter_map(|(idx, group)| {
                    group
                        .first()
                        .and_then(|triple| extract_subject(triple).map(|subj| (subj, idx)))
                })
                .collect();

            // Match each private group to a public knowledge asset
            for private_group in &private_ka_triples_grouped {
                if let Some(first_triple) = private_group.first()
                    && let Some(private_subject) = extract_subject(first_triple)
                {
                    // Try direct subject match first
                    let matched_idx = if let Some(&idx) = public_subject_map.get(private_subject) {
                        Some(idx)
                    } else {
                        // Try matching by hashed subject
                        let subject_without_brackets = private_subject
                            .trim_start_matches('<')
                            .trim_end_matches('>');
                        let hashed_subject = format!(
                            "<{}{}>",
                            PRIVATE_HASH_SUBJECT_PREFIX,
                            crate::managers::blockchain::utils::sha256_hex(
                                subject_without_brackets.as_bytes()
                            )
                        );
                        public_subject_map.get(hashed_subject.as_str()).copied()
                    };

                    // Attach private triples to the matched knowledge asset
                    if let Some(idx) = matched_idx {
                        let private_strings: Vec<String> =
                            private_group.iter().map(|s| s.to_string()).collect();
                        knowledge_assets[idx].set_private_triples(private_strings);
                    }
                }
            }
        }

        knowledge_assets
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::managers::blockchain;

    // Note: Tests for the business logic in build_knowledge_assets:
    // - Private-hash triple separation
    // - UAL generation
    // - Private-to-public subject matching

    #[test]
    fn test_simple_public_only() {
        let kc_ual = "did:dkg:hardhat1:31337/0x123/1";
        let dataset = Assertion {
            public: vec![
                r#"<http://example.org/subject1> <http://example.org/predicate1> "value1" ."#
                    .to_string(),
                r#"<http://example.org/subject1> <http://example.org/predicate2> "value2" ."#
                    .to_string(),
                r#"<http://example.org/subject2> <http://example.org/predicate1> "value3" ."#
                    .to_string(),
            ],
            private: None,
        };

        let kas = TripleStoreService::build_knowledge_assets(kc_ual, &dataset);

        // Expected: 2 KAs (grouped by subject)
        assert_eq!(kas.len(), 2);
        assert_eq!(kas[0].ual(), "did:dkg:hardhat1:31337/0x123/1/1");
        assert_eq!(kas[0].public_triples().len(), 2); // subject1
        assert_eq!(kas[1].ual(), "did:dkg:hardhat1:31337/0x123/1/2");
        assert_eq!(kas[1].public_triples().len(), 1); // subject2
    }

    #[test]
    fn test_with_private_hash_triples() {
        let kc_ual = "did:dkg:hardhat1:31337/0x456/2";
        let hashed_subject =
            blockchain::utils::sha256_hex("http://example.org/private-subject".as_bytes());
        let hash_triple = format!(
            r#"<{}{}> <http://example.org/predicate1> "hashed_value" ."#,
            PRIVATE_HASH_SUBJECT_PREFIX, hashed_subject
        );

        let dataset = Assertion {
            public: vec![
                r#"<http://example.org/subject1> <http://example.org/predicate1> "value1" ."#
                    .to_string(),
                hash_triple,
            ],
            private: None,
        };

        let kas = TripleStoreService::build_knowledge_assets(kc_ual, &dataset);

        // Expected: 2 KAs (regular subject + hash subject)
        assert_eq!(kas.len(), 2);
        assert_eq!(kas[0].ual(), "did:dkg:hardhat1:31337/0x456/2/1");
        assert_eq!(kas[1].ual(), "did:dkg:hardhat1:31337/0x456/2/2");
    }

    #[test]
    fn test_public_and_private_direct_match() {
        let kc_ual = "did:dkg:hardhat1:31337/0x789/3";

        let dataset = Assertion {
            public: vec![
                r#"<http://example.org/asset1> <http://example.org/name> "Asset One" ."#
                    .to_string(),
                r#"<http://example.org/asset1> <http://example.org/type> "Document" ."#.to_string(),
                r#"<http://example.org/asset2> <http://example.org/name> "Asset Two" ."#
                    .to_string(),
            ],
            private: Some(vec![
                r#"<http://example.org/asset1> <http://example.org/secret> "private data" ."#
                    .to_string(),
                r#"<http://example.org/asset2> <http://example.org/secret> "more private" ."#
                    .to_string(),
            ]),
        };

        let kas = TripleStoreService::build_knowledge_assets(kc_ual, &dataset);

        // Expected: 2 KAs, each with matched private triples
        assert_eq!(kas.len(), 2);
        assert_eq!(kas[0].ual(), "did:dkg:hardhat1:31337/0x789/3/1");
        assert_eq!(kas[0].public_triples().len(), 2); // asset1 public
        assert!(kas[0].private_triples().is_some());
        assert_eq!(kas[0].private_triples().unwrap().len(), 1); // asset1 private

        assert_eq!(kas[1].ual(), "did:dkg:hardhat1:31337/0x789/3/2");
        assert_eq!(kas[1].public_triples().len(), 1); // asset2 public
        assert!(kas[1].private_triples().is_some());
        assert_eq!(kas[1].private_triples().unwrap().len(), 1); // asset2 private
    }

    #[test]
    fn test_private_with_hashed_subject_match() {
        let kc_ual = "did:dkg:hardhat1:31337/0xabc/4";
        let private_subject = "http://example.org/hidden-asset";
        let private_subject_hash = blockchain::utils::sha256_hex(private_subject.as_bytes());

        let hash_triple = format!(
            r#"<{}{}> <http://example.org/hash> "hash_placeholder" ."#,
            PRIVATE_HASH_SUBJECT_PREFIX, private_subject_hash
        );

        let private_triple = format!(
            r#"<{}> <http://example.org/secret> "hidden data" ."#,
            private_subject
        );

        let dataset = Assertion {
            public: vec![
                r#"<http://example.org/public-asset> <http://example.org/name> "Public" ."#
                    .to_string(),
                hash_triple,
            ],
            private: Some(vec![private_triple]),
        };

        let kas = TripleStoreService::build_knowledge_assets(kc_ual, &dataset);

        // Expected: 2 KAs, private matches to second (hash placeholder)
        assert_eq!(kas.len(), 2);
        assert_eq!(kas[0].ual(), "did:dkg:hardhat1:31337/0xabc/4/1");
        assert!(kas[0].private_triples().is_none()); // public-asset has no private

        assert_eq!(kas[1].ual(), "did:dkg:hardhat1:31337/0xabc/4/2");
        assert!(kas[1].private_triples().is_some()); // hash placeholder has private
        assert_eq!(kas[1].private_triples().unwrap().len(), 1);
    }

    #[test]
    fn test_multiple_subjects_complex() {
        let kc_ual = "did:dkg:otp:2043/0xdef/5";

        let dataset = Assertion {
            public: vec![
                r#"<http://example.org/person/1> <http://schema.org/name> "Alice" ."#.to_string(),
                r#"<http://example.org/person/1> <http://schema.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> ."#.to_string(),
                r#"<http://example.org/person/2> <http://schema.org/name> "Bob" ."#.to_string(),
                r#"<http://example.org/organization/1> <http://schema.org/name> "Acme Corp" ."#.to_string(),
                r#"<http://example.org/person/1> <http://schema.org/worksFor> <http://example.org/organization/1> ."#.to_string(),
            ],
            private: Some(vec![
                r#"<http://example.org/person/1> <http://example.org/ssn> "123-45-6789" ."#.to_string(),
                r#"<http://example.org/person/2> <http://example.org/ssn> "987-65-4321" ."#.to_string(),
            ]),
        };

        let kas = TripleStoreService::build_knowledge_assets(kc_ual, &dataset);

        // Expected: 3 KAs sorted alphabetically by subject:
        // 1. organization/1 (comes first alphabetically)
        // 2. person/1
        // 3. person/2
        assert_eq!(kas.len(), 3);

        // First KA: organization/1 (alphabetically first)
        assert_eq!(kas[0].ual(), "did:dkg:otp:2043/0xdef/5/1");
        assert_eq!(kas[0].public_triples().len(), 1); // organization/1 has 1 triple
        assert!(kas[0].private_triples().is_none()); // organization/1 has no private

        // Second KA: person/1
        assert_eq!(kas[1].ual(), "did:dkg:otp:2043/0xdef/5/2");
        assert_eq!(kas[1].public_triples().len(), 3); // person/1 has 3 triples
        assert!(kas[1].private_triples().is_some());
        assert_eq!(kas[1].private_triples().unwrap().len(), 1); // person/1 has 1 private

        // Third KA: person/2
        assert_eq!(kas[2].ual(), "did:dkg:otp:2043/0xdef/5/3");
        assert_eq!(kas[2].public_triples().len(), 1); // person/2 has 1 triple
        assert!(kas[2].private_triples().is_some());
        assert_eq!(kas[2].private_triples().unwrap().len(), 1); // person/2 has 1 private
    }
}
