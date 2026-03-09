use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub(crate) mod build_assets;
mod metadata;

use dkg_domain::{
    Assertion, KnowledgeAsset, KnowledgeCollectionMetadata, ParsedUal, TokenIds, Visibility,
    canonical_evm_address,
};
use dkg_repository::{KcChainMetadataEntry, KcChainMetadataRepository};
#[cfg(test)]
use dkg_triple_store::PRIVATE_HASH_SUBJECT_PREFIX;
use dkg_triple_store::{GraphVisibility, TripleStoreManager, error::TripleStoreError};
use futures::{StreamExt, stream};
use tracing::instrument;

use self::metadata::reconstruct_metadata_triples;
use crate::application::state_metadata::{PrivateGraphMode, PrivateGraphPresence};
#[cfg(test)]
use crate::application::state_metadata::{decode_sparse_ids, encode_bitmap, encode_sparse_ids};

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
pub(crate) struct TripleStoreAssertions {
    triple_store_manager: Arc<TripleStoreManager>,
    kc_chain_metadata_repository: KcChainMetadataRepository,
}

impl TripleStoreAssertions {
    pub(crate) fn new(
        triple_store_manager: Arc<TripleStoreManager>,
        kc_chain_metadata_repository: KcChainMetadataRepository,
    ) -> Self {
        Self {
            triple_store_manager,
            kc_chain_metadata_repository,
        }
    }

    /// Query assertion data from the triple store.
    ///
    /// Follows the same logic as JS tripleStoreService.getAssertion:
    /// - For single KA: query named graph directly
    /// - For collection: query all named graphs in requested token range
    ///
    /// Returns the query result with public, private, and metadata triples,
    /// or None if not found. Errors are propagated to the caller.
    pub(crate) async fn query_assertion(
        &self,
        parsed_ual: &ParsedUal,
        token_ids: &TokenIds,
        visibility: Visibility,
        include_metadata: bool,
    ) -> Result<Option<AssertionQueryResult>, TripleStoreError> {
        // Query assertion and metadata in parallel when metadata is requested
        let (assertion, metadata) = if include_metadata {
            let assertion_future = async {
                self.query_assertion_data(parsed_ual, token_ids, visibility)
                    .await
            };

            let metadata_future = self.query_metadata(parsed_ual, token_ids);

            tokio::join!(assertion_future, metadata_future)
        } else {
            let assertion = self
                .query_assertion_data(parsed_ual, token_ids, visibility)
                .await;
            (assertion, Ok(None))
        };

        let assertion = assertion?;
        let metadata = metadata?;

        // Check if we found any data
        let Some(assertion) = assertion else {
            return Ok(None);
        };
        if !assertion.has_data() {
            return Ok(None);
        }

        Ok(Some(AssertionQueryResult {
            assertion,
            metadata,
        }))
    }

    async fn query_assertion_data(
        &self,
        parsed_ual: &ParsedUal,
        token_ids: &TokenIds,
        visibility: Visibility,
    ) -> Result<Option<Assertion>, TripleStoreError> {
        if parsed_ual.knowledge_asset_id.is_some() {
            let assertion = self
                .query_single_asset(&parsed_ual.to_ual_string(), visibility)
                .await?;
            Ok(Some(assertion))
        } else {
            let kc_ual = parsed_ual.knowledge_collection_ual();
            self.query_collection(&kc_ual, token_ids, visibility).await
        }
    }

    /// Query a single knowledge asset.
    async fn query_single_asset(
        &self,
        ka_ual: &str,
        visibility: Visibility,
    ) -> Result<Assertion, TripleStoreError> {
        let need_public = visibility == Visibility::Public || visibility == Visibility::All;
        let need_private = visibility == Visibility::Private || visibility == Visibility::All;

        // Query public and private triples in parallel when both are needed
        let (public, private) = tokio::join!(
            async {
                if need_public {
                    self.triple_store_manager
                        .get_knowledge_asset_named_graph(ka_ual, GraphVisibility::Public)
                        .await
                        .map(Some)
                } else {
                    Ok(None)
                }
            },
            async {
                if need_private {
                    self.triple_store_manager
                        .get_knowledge_asset_named_graph(ka_ual, GraphVisibility::Private)
                        .await
                        .map(|triples| {
                            if triples.is_empty() {
                                None
                            } else {
                                Some(triples)
                            }
                        })
                } else {
                    Ok(None)
                }
            }
        );

        let public = public?.unwrap_or_default();
        let private = private?;

        Ok(Assertion::new(public, private))
    }

    /// Query a knowledge collection.
    async fn query_collection(
        &self,
        kc_ual: &str,
        token_ids: &TokenIds,
        visibility: Visibility,
    ) -> Result<Option<Assertion>, TripleStoreError> {
        let need_public = visibility == Visibility::Public || visibility == Visibility::All;
        let need_private = visibility == Visibility::Private || visibility == Visibility::All;

        // Query public and private triples in parallel when both are needed
        let (public, private) = tokio::join!(
            async {
                if need_public {
                    self.triple_store_manager
                        .get_knowledge_collection_named_graphs(
                            kc_ual,
                            token_ids.start_token_id(),
                            token_ids.end_token_id(),
                            token_ids.burned(),
                            GraphVisibility::Public,
                        )
                        .await
                        .map(Some)
                } else {
                    Ok(None)
                }
            },
            async {
                if need_private {
                    self.triple_store_manager
                        .get_knowledge_collection_named_graphs(
                            kc_ual,
                            token_ids.start_token_id(),
                            token_ids.end_token_id(),
                            token_ids.burned(),
                            GraphVisibility::Private,
                        )
                        .await
                        .map(|triples| {
                            if triples.is_empty() {
                                None
                            } else {
                                Some(triples)
                            }
                        })
                } else {
                    Ok(None)
                }
            }
        );

        let public = public?.unwrap_or_default();
        let private = private?;

        Ok(Some(Assertion::new(public, private)))
    }

    /// Query metadata for a knowledge collection.
    ///
    /// Metadata is reconstructed from SQL metadata + private graph encoding.
    /// SQL is treated as the source of truth for serving metadata.
    async fn query_metadata(
        &self,
        parsed_ual: &ParsedUal,
        token_ids: &TokenIds,
    ) -> Result<Option<Vec<String>>, TripleStoreError> {
        Ok(self.query_metadata_from_sql(parsed_ual, token_ids).await)
    }

    async fn query_metadata_from_sql(
        &self,
        parsed_ual: &ParsedUal,
        token_ids: &TokenIds,
    ) -> Option<Vec<String>> {
        let Ok(kc_id) = u64::try_from(parsed_ual.knowledge_collection_id) else {
            return None;
        };

        let blockchain_id = parsed_ual.blockchain.as_str();
        let contract_address = canonical_evm_address(&parsed_ual.contract);
        let entry = match self
            .kc_chain_metadata_repository
            .get_complete(blockchain_id, &contract_address, kc_id)
            .await
        {
            Ok(Some(entry)) => entry,
            Ok(None) => return None,
            Err(error) => {
                tracing::warn!(
                    blockchain_id = blockchain_id,
                    contract_address = %contract_address,
                    kc_id = kc_id,
                    error = %error,
                    "Failed to read KC metadata from repository"
                );
                return None;
            }
        };

        Self::reconstruct_metadata_from_entry(parsed_ual, token_ids, &entry)
    }

    /// Query assertion data for multiple UALs in batch.
    ///
    /// Queries all UALs concurrently, collecting results into a HashMap.
    /// Only UALs that have data are included in the result.
    ///
    /// This method is used by the batch get protocol to efficiently query
    /// multiple assets in a single operation. Concurrency is controlled by
    /// the semaphore in TripleStoreManager.
    pub(crate) async fn query_assertions_batch(
        &self,
        uals_with_token_ids: Vec<(ParsedUal, TokenIds)>,
        visibility: Visibility,
        include_metadata: bool,
    ) -> Result<HashMap<String, AssertionQueryResult>, TripleStoreError> {
        let metadata_by_ual = if include_metadata {
            self.query_batch_metadata_from_sql(&uals_with_token_ids)
                .await
        } else {
            HashMap::new()
        };

        let max_in_flight = self.triple_store_manager.max_concurrent_operations();

        // Execute queries with bounded fan-out to avoid unbounded permit queuing.
        let mut query_stream = stream::iter(uals_with_token_ids.into_iter())
            .map(|(parsed_ual, token_ids)| async move {
                let ual_string = parsed_ual.to_ual_string();
                let result = self
                    .query_assertion_data(&parsed_ual, &token_ids, visibility)
                    .await;
                (ual_string, result)
            })
            .buffer_unordered(max_in_flight.max(1));

        // Collect successful results with data
        let mut results_map = HashMap::new();
        let mut first_error: Option<TripleStoreError> = None;

        while let Some((ual_string, result)) = query_stream.next().await {
            match result {
                Ok(Some(assertion)) if assertion.has_data() => {
                    let metadata = metadata_by_ual.get(&ual_string).cloned();
                    results_map.insert(
                        ual_string,
                        AssertionQueryResult {
                            assertion,
                            metadata,
                        },
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }

        Ok(results_map)
    }

    /// Insert a knowledge collection using pre-built knowledge assets.
    #[instrument(
        name = "triple_store_insert",
        skip(self, knowledge_assets, metadata),
        fields(ual = %knowledge_collection_ual)
    )]
    pub(crate) async fn insert_knowledge_collection_assets(
        &self,
        knowledge_collection_ual: &str,
        knowledge_assets: &[KnowledgeAsset],
        metadata: Option<&KnowledgeCollectionMetadata>,
        paranet_ual: Option<&str>,
    ) -> Result<usize, TripleStoreError> {
        self.triple_store_manager
            .insert_knowledge_collection(
                knowledge_collection_ual,
                knowledge_assets,
                metadata,
                paranet_ual,
            )
            .await
    }

    fn reconstruct_metadata_from_entry(
        parsed_ual: &ParsedUal,
        token_ids: &TokenIds,
        entry: &KcChainMetadataEntry,
    ) -> Option<Vec<String>> {
        let mode = PrivateGraphMode::from_raw(entry.private_graph_mode?)?;
        let private_presence = PrivateGraphPresence::from_mode_and_payload(
            mode,
            entry.private_graph_payload.as_deref(),
        )?;

        Some(reconstruct_metadata_triples(
            parsed_ual,
            token_ids,
            entry,
            &private_presence,
        ))
    }

    async fn query_batch_metadata_from_sql(
        &self,
        uals_with_token_ids: &[(ParsedUal, TokenIds)],
    ) -> HashMap<String, Vec<String>> {
        let mut grouped_kc_ids: HashMap<(String, String), HashSet<u64>> = HashMap::new();
        for (parsed_ual, _) in uals_with_token_ids {
            let Ok(kc_id) = u64::try_from(parsed_ual.knowledge_collection_id) else {
                continue;
            };
            let blockchain_id = parsed_ual.blockchain.to_string();
            let contract_address = canonical_evm_address(&parsed_ual.contract);
            grouped_kc_ids
                .entry((blockchain_id, contract_address))
                .or_default()
                .insert(kc_id);
        }

        let mut entries_by_key: HashMap<(String, String, u64), KcChainMetadataEntry> =
            HashMap::new();
        for ((blockchain_id, contract_address), kc_ids) in grouped_kc_ids {
            let kc_ids = kc_ids.into_iter().collect::<Vec<_>>();
            match self
                .kc_chain_metadata_repository
                .get_many_complete(&blockchain_id, &contract_address, &kc_ids)
                .await
            {
                Ok(entries) => {
                    for (kc_id, entry) in entries {
                        entries_by_key.insert(
                            (blockchain_id.clone(), contract_address.clone(), kc_id),
                            entry,
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        blockchain_id = blockchain_id,
                        contract_address = %contract_address,
                        error = %error,
                        "Failed to read batched KC metadata from repository"
                    );
                }
            }
        }

        let mut metadata_by_ual = HashMap::new();
        for (parsed_ual, token_ids) in uals_with_token_ids {
            let Ok(kc_id) = u64::try_from(parsed_ual.knowledge_collection_id) else {
                continue;
            };
            let blockchain_id = parsed_ual.blockchain.to_string();
            let contract_address = canonical_evm_address(&parsed_ual.contract);
            let key = (blockchain_id, contract_address, kc_id);

            let Some(entry) = entries_by_key.get(&key) else {
                continue;
            };

            if let Some(metadata) =
                Self::reconstruct_metadata_from_entry(parsed_ual, token_ids, entry)
            {
                metadata_by_ual.insert(parsed_ual.to_ual_string(), metadata);
            }
        }

        metadata_by_ual
    }

    /// Check which knowledge collections exist by UAL (batched).
    ///
    /// Returns the subset of UALs that exist in the metadata graph.
    pub(crate) async fn knowledge_collections_exist_by_uals(
        &self,
        kc_uals: &[String],
    ) -> std::collections::HashSet<String> {
        match self
            .triple_store_manager
            .knowledge_collections_exist_by_uals(kc_uals)
            .await
        {
            Ok(existing) => existing,
            Err(error) => {
                tracing::warn!(
                    requested = kc_uals.len(),
                    error = %error,
                    "Failed to query existing knowledge collections by UALs"
                );
                std::collections::HashSet::new()
            }
        }
    }

    /// Strict variant of KC UAL existence check.
    ///
    /// Returns backend errors instead of swallowing them and returning an empty set.
    pub(crate) async fn try_knowledge_collections_exist_by_uals(
        &self,
        kc_uals: &[String],
    ) -> Result<std::collections::HashSet<String>, TripleStoreError> {
        self.triple_store_manager
            .knowledge_collections_exist_by_uals(kc_uals)
            .await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use dkg_blockchain as blockchain;

    use super::*;
    use crate::application::assertions::build_assets::build_knowledge_assets;

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

        let kas = build_knowledge_assets(kc_ual, &dataset)
            .expect("Expected knowledge asset build to succeed");

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
            blockchain::sha256_hex("http://example.org/private-subject".as_bytes());
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

        let kas = build_knowledge_assets(kc_ual, &dataset)
            .expect("Expected knowledge asset build to succeed");

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

        let kas = build_knowledge_assets(kc_ual, &dataset)
            .expect("Expected knowledge asset build to succeed");

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
        let private_subject_hash = blockchain::sha256_hex(private_subject.as_bytes());

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

        let kas = build_knowledge_assets(kc_ual, &dataset)
            .expect("Expected knowledge asset build to succeed");

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

        let kas = build_knowledge_assets(kc_ual, &dataset)
            .expect("Expected knowledge asset build to succeed");

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

    #[test]
    fn private_graph_sparse_roundtrip() {
        let ids = vec![1_u64, 7_u64, 1024_u64];
        let encoded = encode_sparse_ids(&ids);
        let decoded = decode_sparse_ids(&encoded).expect("sparse payload should decode");
        assert_eq!(decoded.len(), ids.len());
        for id in ids {
            assert!(decoded.contains(&id));
        }
    }

    #[test]
    fn private_graph_bitmap_detects_membership() {
        let encoded = encode_bitmap(&[1, 3, 8, 9], 9);
        let presence = PrivateGraphPresence::from_mode_and_payload(
            PrivateGraphMode::Bitmap,
            Some(encoded.as_slice()),
        )
        .expect("bitmap payload should decode");

        assert!(presence.has_private_graph(1));
        assert!(presence.has_private_graph(3));
        assert!(presence.has_private_graph(8));
        assert!(presence.has_private_graph(9));
        assert!(!presence.has_private_graph(2));
        assert!(!presence.has_private_graph(10));
    }
}
