use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dkg_domain::{
    Assertion, KnowledgeAsset, KnowledgeCollectionMetadata, ParsedUal, TokenIds, Visibility,
    parse_ual,
};
use dkg_repository::{KcChainMetadataEntry, KcChainMetadataRepository};
use dkg_triple_store::{
    GraphVisibility, MetadataAsset, MetadataTriples, PRIVATE_HASH_SUBJECT_PREFIX,
    TripleStoreManager, error::TripleStoreError, extract_subject, group_triples_by_subject,
};
use futures::{StreamExt, stream};
use tracing::instrument;

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
    /// - For collection: check if first/last KA exists, then query all named graphs
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
        let exists = self
            .knowledge_collection_exists(
                kc_ual,
                token_ids.start_token_id(),
                token_ids.end_token_id(),
                token_ids.burned(),
            )
            .await?;

        if !exists {
            tracing::debug!(
                kc_ual = %kc_ual,
                "Knowledge collection does not exist locally"
            );
            return Ok(None);
        }

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
        let contract_address = format!("{:?}", parsed_ual.contract);
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

        let mode = PrivateGraphMode::from_raw(entry.private_graph_mode?)?;
        let private_presence = PrivateGraphPresence::from_mode_and_payload(
            mode,
            entry.private_graph_payload.as_deref(),
        )?;

        Some(Self::reconstruct_metadata_triples(
            parsed_ual,
            token_ids,
            &entry,
            &private_presence,
        ))
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
        uals_with_token_ids: &[(ParsedUal, TokenIds)],
        visibility: Visibility,
        include_metadata: bool,
    ) -> Result<HashMap<String, AssertionQueryResult>, TripleStoreError> {
        let max_in_flight = self.triple_store_manager.max_concurrent_operations();
        let queries: Vec<_> = uals_with_token_ids
            .iter()
            .map(|(parsed_ual, token_ids)| {
                (
                    parsed_ual.to_ual_string(),
                    parsed_ual.clone(),
                    token_ids.clone(),
                )
            })
            .collect();

        // Execute queries with bounded fan-out to avoid unbounded permit queuing.
        let query_results = stream::iter(queries.into_iter())
            .map(|(ual_string, parsed_ual, token_ids)| async move {
                let result = self
                    .query_assertion(&parsed_ual, &token_ids, visibility, include_metadata)
                    .await;
                (ual_string, result)
            })
            .buffer_unordered(max_in_flight.max(1))
            .collect::<Vec<_>>()
            .await;

        // Collect successful results with data
        let mut results_map = HashMap::new();
        let mut first_error: Option<TripleStoreError> = None;

        for (ual_string, result) in query_results {
            match result {
                Ok(Some(r)) if r.assertion.has_data() => {
                    results_map.insert(ual_string, r);
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

    /// Insert a knowledge collection into the triple store.
    ///
    /// This function:
    /// 1. Separates public triples into regular and private-hash triples
    /// 2. Groups triples by subject to form knowledge assets
    /// 3. Matches private triples to their corresponding public knowledge assets
    /// 4. Delegates RDF serialization and SPARQL building to TripleStoreManager
    ///
    /// Returns the total number of triples inserted, or an error.
    #[instrument(
        name = "triple_store_insert",
        skip(self, dataset, metadata),
        fields(ual = %knowledge_collection_ual)
    )]
    pub(crate) async fn insert_knowledge_collection(
        &self,
        knowledge_collection_ual: &str,
        dataset: &Assertion,
        metadata: &Option<KnowledgeCollectionMetadata>,
        paranet_ual: Option<&str>,
    ) -> Result<usize, TripleStoreError> {
        // Build knowledge assets from the dataset
        let knowledge_assets = Self::build_knowledge_assets(knowledge_collection_ual, dataset)?;
        let private_graph_encoding = Self::encode_private_graph_presence(&knowledge_assets);

        // Delegate to the triple store manager for RDF serialization and insertion
        let inserted = self
            .triple_store_manager
            .insert_knowledge_collection(
                knowledge_collection_ual,
                &knowledge_assets,
                metadata,
                paranet_ual,
            )
            .await?;

        if let Ok(parsed_ual) = parse_ual(knowledge_collection_ual)
            && let Ok(kc_id) = u64::try_from(parsed_ual.knowledge_collection_id)
        {
            let contract_address = format!("{:?}", parsed_ual.contract);
            if let Err(error) = self
                .kc_chain_metadata_repository
                .upsert_private_graph_encoding(
                    parsed_ual.blockchain.as_str(),
                    &contract_address,
                    kc_id,
                    Some(private_graph_encoding.mode as u32),
                    private_graph_encoding.payload.as_deref(),
                    Some("triple_store_insert"),
                )
                .await
            {
                tracing::warn!(
                    blockchain_id = %parsed_ual.blockchain,
                    contract_address = %contract_address,
                    kc_id = kc_id,
                    error = %error,
                    "Failed to persist private graph encoding"
                );
            }
        }

        Ok(inserted)
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
        burned: &[u64],
    ) -> Result<bool, TripleStoreError> {
        let burned_set: std::collections::HashSet<u64> = burned.iter().copied().collect();

        let mut first = start_token_id;
        while first <= end_token_id && burned_set.contains(&first) {
            first += 1;
        }
        if first > end_token_id {
            return Ok(false);
        }

        let mut last = end_token_id;
        loop {
            if !burned_set.contains(&last) {
                break;
            }
            if last == 0 || last <= first {
                return Ok(false);
            }
            last -= 1;
        }

        let first_ka_ual = format!("{}/{}/public", kc_ual, first);
        let last_ka_ual = format!("{}/{}/public", kc_ual, last);

        let (first_exists, last_exists) = tokio::join!(
            self.triple_store_manager
                .knowledge_asset_exists(&first_ka_ual),
            self.triple_store_manager
                .knowledge_asset_exists(&last_ka_ual)
        );

        Ok(first_exists? && last_exists?)
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
    ) -> Result<Vec<KnowledgeAsset>, TripleStoreError> {
        let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);
        let normalized_public = normalize_triple_lines(&dataset.public);

        // Separate public triples: regular public vs private-hash triples
        let mut filtered_public: Vec<String> = Vec::new();
        let mut private_hash_triples: Vec<String> = Vec::new();

        for triple in normalized_public {
            if triple.starts_with(&private_hash_prefix) {
                private_hash_triples.push(triple);
            } else {
                filtered_public.push(triple);
            }
        }

        // Group public triples by subject, then append private-hash groups
        let mut public_ka_triples_grouped =
            group_triples_by_subject(&filtered_public).map_err(|error| {
                TripleStoreError::ParseError {
                    reason: format!("Failed to group public triples by parsed subject: {error}"),
                }
            })?;
        public_ka_triples_grouped.extend(group_triples_by_subject(&private_hash_triples).map_err(
            |error| TripleStoreError::ParseError {
                reason: format!("Failed to group private-hash triples by parsed subject: {error}"),
            },
        )?);

        // Build a map from public subject -> index for matching private triples later.
        let public_subject_map: HashMap<String, usize> = public_ka_triples_grouped
            .iter()
            .enumerate()
            .filter_map(|(idx, group)| {
                group
                    .first()
                    .and_then(|triple| extract_subject(triple).map(|subj| (subj.to_string(), idx)))
            })
            .collect();

        // Create knowledge assets with public triples, moving grouped vectors to avoid cloning.
        let mut knowledge_assets: Vec<KnowledgeAsset> = public_ka_triples_grouped
            .into_iter()
            .enumerate()
            .map(|(i, triples)| {
                let ual = format!("{}/{}", knowledge_collection_ual, i + 1);
                KnowledgeAsset::new(ual, triples)
            })
            .collect();

        // Match and attach private triples if present
        if let Some(private_triples) = &dataset.private
            && !private_triples.is_empty()
        {
            let normalized_private = normalize_triple_lines(private_triples);
            let private_ka_triples_grouped = group_triples_by_subject(&normalized_private)
                .map_err(|error| TripleStoreError::ParseError {
                    reason: format!("Failed to group private triples by parsed subject: {error}"),
                })?;

            // Match each private group to a public knowledge asset
            for private_group in private_ka_triples_grouped {
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
                            dkg_blockchain::sha256_hex(subject_without_brackets.as_bytes())
                        );
                        public_subject_map.get(hashed_subject.as_str()).copied()
                    };

                    // Attach private triples to the matched knowledge asset (append if already set)
                    if let Some(idx) = matched_idx {
                        match knowledge_assets[idx].private_triples.as_mut() {
                            Some(existing) => existing.extend(private_group),
                            None => knowledge_assets[idx].set_private_triples(private_group),
                        }
                    }
                }
            }
        }

        Ok(knowledge_assets)
    }

    fn encode_private_graph_presence(knowledge_assets: &[KnowledgeAsset]) -> PrivateGraphEncoding {
        if knowledge_assets.is_empty() {
            return PrivateGraphEncoding::none();
        }

        let mut token_ids: Vec<u64> = Vec::with_capacity(knowledge_assets.len());
        let mut private_ids: Vec<u64> = Vec::new();

        for ka in knowledge_assets {
            let Some(token_id) = parse_token_id_from_ka_ual(ka.ual()) else {
                return PrivateGraphEncoding::none();
            };
            token_ids.push(token_id);

            if ka
                .private_triples()
                .is_some_and(|triples| !triples.is_empty())
            {
                private_ids.push(token_id);
            }
        }

        token_ids.sort_unstable();
        token_ids.dedup();
        private_ids.sort_unstable();
        private_ids.dedup();

        let total = token_ids.len();
        let private = private_ids.len();

        if private == 0 {
            return PrivateGraphEncoding::none();
        }
        if private == total {
            return PrivateGraphEncoding::all();
        }
        if private <= 256 {
            return PrivateGraphEncoding {
                mode: PrivateGraphMode::SparseIds,
                payload: Some(encode_sparse_ids(&private_ids)),
            };
        }

        let max_token_id = token_ids.into_iter().max().unwrap_or_default();
        PrivateGraphEncoding {
            mode: PrivateGraphMode::Bitmap,
            payload: Some(encode_bitmap(&private_ids, max_token_id)),
        }
    }

    fn reconstruct_metadata_triples(
        parsed_ual: &ParsedUal,
        token_ids: &TokenIds,
        metadata: &KcChainMetadataEntry,
        private_presence: &PrivateGraphPresence,
    ) -> Vec<String> {
        let kc_ual = parsed_ual.knowledge_collection_ual();
        let burned: HashSet<u64> = token_ids.burned().iter().copied().collect();

        let mut metadata_assets = Vec::new();
        for token_id in token_ids.start_token_id()..=token_ids.end_token_id() {
            if burned.contains(&token_id) {
                continue;
            }
            metadata_assets.push(MetadataAsset {
                ka_ual: format!("{kc_ual}/{token_id}"),
                has_private_graph: private_presence.has_private_graph(token_id),
            });
        }

        let kc_metadata = KnowledgeCollectionMetadata::new(
            metadata.publisher_address.clone(),
            metadata.block_number,
            metadata.transaction_hash.clone(),
            metadata.block_timestamp,
        );
        let built = MetadataTriples::build(&kc_ual, &metadata_assets, Some(&kc_metadata));
        built.all_triples()
    }
}
const MAX_KA_TOKENS_PER_COLLECTION: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrivateGraphMode {
    None = 0,
    All = 1,
    SparseIds = 2,
    Bitmap = 3,
}

impl PrivateGraphMode {
    fn from_raw(raw: u32) -> Option<Self> {
        match raw {
            0 => Some(Self::None),
            1 => Some(Self::All),
            2 => Some(Self::SparseIds),
            3 => Some(Self::Bitmap),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct PrivateGraphEncoding {
    mode: PrivateGraphMode,
    payload: Option<Vec<u8>>,
}

impl PrivateGraphEncoding {
    fn none() -> Self {
        Self {
            mode: PrivateGraphMode::None,
            payload: None,
        }
    }

    fn all() -> Self {
        Self {
            mode: PrivateGraphMode::All,
            payload: None,
        }
    }
}

#[derive(Debug, Clone)]
enum PrivateGraphPresence {
    None,
    All,
    Sparse(HashSet<u64>),
    Bitmap(Vec<u8>),
}

impl PrivateGraphPresence {
    fn from_mode_and_payload(mode: PrivateGraphMode, payload: Option<&[u8]>) -> Option<Self> {
        match mode {
            PrivateGraphMode::None => Some(Self::None),
            PrivateGraphMode::All => Some(Self::All),
            PrivateGraphMode::SparseIds => {
                let payload = payload?;
                decode_sparse_ids(payload).map(Self::Sparse)
            }
            PrivateGraphMode::Bitmap => payload.map(|p| Self::Bitmap(p.to_vec())),
        }
    }

    fn has_private_graph(&self, token_id: u64) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::Sparse(ids) => ids.contains(&token_id),
            Self::Bitmap(bits) => {
                if token_id == 0 {
                    return false;
                }
                let bit_index = usize::try_from(token_id - 1).unwrap_or(usize::MAX);
                let byte_index = bit_index / 8;
                if byte_index >= bits.len() {
                    return false;
                }
                let mask = 1u8 << (bit_index % 8);
                bits[byte_index] & mask != 0
            }
        }
    }
}

fn parse_token_id_from_ka_ual(ka_ual: &str) -> Option<u64> {
    ka_ual.rsplit('/').next()?.parse::<u64>().ok()
}

fn encode_sparse_ids(ids: &[u64]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(ids.len() * 8);
    for id in ids {
        payload.extend_from_slice(&id.to_le_bytes());
    }
    payload
}

fn decode_sparse_ids(payload: &[u8]) -> Option<HashSet<u64>> {
    if !payload.len().is_multiple_of(8) {
        return None;
    }

    let mut ids = HashSet::with_capacity(payload.len() / 8);
    for chunk in payload.chunks_exact(8) {
        let id = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        ids.insert(id);
    }
    Some(ids)
}

fn encode_bitmap(ids: &[u64], max_token_id: u64) -> Vec<u8> {
    let capped = max_token_id.min(MAX_KA_TOKENS_PER_COLLECTION);
    let max_index = usize::try_from(capped).unwrap_or(0);
    let mut bytes = vec![0u8; max_index.div_ceil(8)];

    for id in ids {
        if *id == 0 {
            continue;
        }
        let bit_index = usize::try_from(*id - 1).unwrap_or(usize::MAX);
        let byte_index = bit_index / 8;
        if byte_index >= bytes.len() {
            continue;
        }
        bytes[byte_index] |= 1u8 << (bit_index % 8);
    }

    bytes
}

fn normalize_triple_lines(triples: &[String]) -> Vec<String> {
    triples
        .iter()
        .flat_map(|entry| entry.lines())
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use dkg_blockchain as blockchain;

    use super::*;

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

        let kas = TripleStoreAssertions::build_knowledge_assets(kc_ual, &dataset)
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

        let kas = TripleStoreAssertions::build_knowledge_assets(kc_ual, &dataset)
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

        let kas = TripleStoreAssertions::build_knowledge_assets(kc_ual, &dataset)
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

        let kas = TripleStoreAssertions::build_knowledge_assets(kc_ual, &dataset)
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

        let kas = TripleStoreAssertions::build_knowledge_assets(kc_ual, &dataset)
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
