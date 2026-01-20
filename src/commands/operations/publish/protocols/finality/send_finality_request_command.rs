use std::{collections::HashMap, sync::Arc};

use blockchain::{Address, BlockchainId, BlockchainManager, H256, U256};
use network::{
    NetworkManager, PeerId, RequestMessage,
    message::{RequestMessageHeader, RequestMessageType},
};
use repository::RepositoryManager;
use triple_store::{
    KnowledgeAsset, KnowledgeCollectionMetadata, TripleStoreManager, extract_subject,
    group_nquads_by_subject, query::subjects::PRIVATE_HASH_SUBJECT_PREFIX,
};
use uuid::Uuid;
use validation::ValidationManager;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    controllers::rpc_controller::{
        NetworkProtocols, ProtocolRequest, messages::FinalityRequestData,
    },
    error::NodeError,
    services::pending_storage_service::PendingStorageService,
    types::models::Assertion,
    utils::ual::derive_ual,
};

/// Raw event data from KnowledgeCollectionCreated event.
/// Parsing and validation happens in the command handler, not the event listener.
#[derive(Clone)]
pub struct SendFinalityRequestCommandData {
    /// The blockchain where the event was emitted
    pub blockchain: BlockchainId,
    /// The publish operation ID (raw string from event, parsed to UUID in handler)
    pub publish_operation_id: String,
    /// The on-chain knowledge collection ID (raw U256 from event)
    pub knowledge_collection_id: U256,
    /// The KnowledgeCollectionStorage contract address
    pub knowledge_collection_storage_address: Address,
    /// The byte size of the knowledge collection
    pub byte_size: u128,
    /// The merkle root (dataset root) of the knowledge collection
    pub dataset_root: H256,
    /// The transaction hash (used to fetch publisher address)
    pub transaction_hash: H256,
    /// The block number where the event was emitted
    pub block_number: u64,
    /// The block timestamp (unix seconds)
    pub block_timestamp: u64,
}

impl SendFinalityRequestCommandData {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockchain: BlockchainId,
        publish_operation_id: String,
        knowledge_collection_id: U256,
        knowledge_collection_storage_address: Address,
        byte_size: u128,
        dataset_root: H256,
        transaction_hash: H256,
        block_number: u64,
        block_timestamp: u64,
    ) -> Self {
        Self {
            blockchain,
            publish_operation_id,
            knowledge_collection_id,
            knowledge_collection_storage_address,
            byte_size,
            dataset_root,
            transaction_hash,
            block_number,
            block_timestamp,
        }
    }
}

pub struct SendFinalityRequestCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    blockchain_manager: Arc<BlockchainManager>,
    validation_manager: Arc<ValidationManager>,
    pending_storage_service: Arc<PendingStorageService>,
    triple_store_manager: Arc<TripleStoreManager>,
}

impl SendFinalityRequestCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            validation_manager: Arc::clone(context.validation_manager()),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
            triple_store_manager: Arc::clone(context.triple_store_manager()),
        }
    }

    /// Inserts a knowledge collection into the triple store.
    ///
    /// This function:
    /// 1. Separates public triples into regular and private-hash triples
    /// 2. Groups triples by subject to form knowledge assets
    /// 3. Matches private triples to their corresponding public knowledge assets
    /// 4. Delegates RDF serialization and SPARQL building to TripleStoreManager
    ///
    /// Returns the total number of triples inserted, or an error.
    async fn insert_knowledge_collection(
        &self,
        operation_id: Uuid,
        publish_operation_id: Uuid,
        knowledge_collection_ual: &str,
        dataset: &Assertion,
        metadata: &KnowledgeCollectionMetadata,
    ) -> Result<usize, NodeError> {
        tracing::info!(
            operation_id = %operation_id,
            publish_operation_id = %publish_operation_id,
            ual = %knowledge_collection_ual,
            "Inserting Knowledge Collection to the Triple Store's dkg repository."
        );

        // Build knowledge assets from the dataset
        let knowledge_assets = self.build_knowledge_assets(knowledge_collection_ual, dataset);

        // Delegate to the triple store manager for RDF serialization and insertion
        let total_triples = self
            .triple_store_manager
            .insert_knowledge_collection(knowledge_collection_ual, &knowledge_assets, metadata)
            .await?;

        tracing::info!(
            operation_id = %operation_id,
            ual = %knowledge_collection_ual,
            total_triples = total_triples,
            "Knowledge Collection has been successfully inserted to the Triple Store."
        );

        Ok(total_triples)
    }

    /// Build knowledge assets from a dataset.
    ///
    /// This contains the DKG business logic:
    /// - Separating public triples from private-hash triples
    /// - Grouping triples by subject
    /// - Generating UALs for each knowledge asset
    /// - Matching private triples to public knowledge assets
    fn build_knowledge_assets(
        &self,
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
                            blockchain::utils::sha256_hex(subject_without_brackets.as_bytes())
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

impl CommandHandler<SendFinalityRequestCommandData> for SendFinalityRequestCommandHandler {
    async fn execute(&self, data: &SendFinalityRequestCommandData) -> CommandExecutionResult {
        // Generate a new operation ID for the finality request
        let operation_id = Uuid::new_v4();
        // Parse the operation ID from the raw string
        let publish_operation_id = match Uuid::parse_str(&data.publish_operation_id) {
            Ok(uuid) => uuid,
            Err(e) => {
                tracing::error!(
                    publish_operation_id = %data.publish_operation_id,
                    error = %e,
                    "Failed to parse publish_operation_id as UUID"
                );
                return CommandExecutionResult::Completed;
            }
        };

        // Convert knowledge_collection_id from U256 to u128
        let knowledge_collection_id: u128 = match data.knowledge_collection_id.try_into() {
            Ok(id) => id,
            Err(_) => {
                tracing::error!(
                    knowledge_collection_id = %data.knowledge_collection_id,
                    "Knowledge collection ID exceeds u128 max"
                );
                return CommandExecutionResult::Completed;
            }
        };

        // Fetch the publisher address from the transaction
        let publisher_address = match self
            .blockchain_manager
            .get_transaction_sender(&data.blockchain, data.transaction_hash)
            .await
        {
            Ok(Some(addr)) => addr,
            Ok(None) => {
                tracing::error!(
                    tx_hash = %data.transaction_hash,
                    "Transaction not found, cannot determine publisher address"
                );
                return CommandExecutionResult::Completed;
            }
            Err(e) => {
                tracing::error!(
                    tx_hash = %data.transaction_hash,
                    error = %e,
                    "Failed to fetch transaction"
                );
                return CommandExecutionResult::Completed;
            }
        };

        tracing::info!(
            operation_id = %operation_id,
            blockchain = %data.blockchain,
            knowledge_collection_id = knowledge_collection_id,
            byte_size = data.byte_size,
            publisher = %publisher_address,
            block_number = data.block_number,
            "Processing FinalizePublishOperation"
        );

        // Retrieve cached dataset from pending storage
        let pending_data = match self
            .pending_storage_service
            .get_dataset(publish_operation_id)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    error = %e,
                    "Failed to retrieve dataset from pending storage"
                );
                return CommandExecutionResult::Completed;
            }
        };

        // Validate merkle root matches
        let blockchain_merkle_root =
            format!("0x{}", blockchain::utils::to_hex_string(data.dataset_root));
        if blockchain_merkle_root != pending_data.dataset_root() {
            tracing::error!(
                operation_id = %operation_id,
                blockchain_merkle_root = %blockchain_merkle_root,
                cached_merkle_root = %pending_data.dataset_root(),
                "Merkle root mismatch: blockchain value does not match cached value"
            );
            return CommandExecutionResult::Completed;
        }

        // Validate byte size matches
        let calculated_size = self
            .validation_manager
            .calculate_assertion_size(&pending_data.dataset().public);
        if data.byte_size != calculated_size as u128 {
            tracing::error!(
                operation_id = %operation_id,
                blockchain_byte_size = data.byte_size,
                calculated_byte_size = calculated_size,
                "Byte size mismatch: blockchain value does not match calculated value"
            );
            return CommandExecutionResult::Completed;
        }

        tracing::debug!(
            operation_id = %operation_id,
            "Publish data validation successful"
        );

        let metadata = KnowledgeCollectionMetadata::new(
            publisher_address.to_string().to_lowercase(),
            data.block_number,
            data.transaction_hash.to_string(),
            data.block_timestamp,
        );

        // Derive UAL for the knowledge collection
        let ual = derive_ual(
            &data.blockchain,
            &data.knowledge_collection_storage_address,
            knowledge_collection_id,
            None,
        );

        let total_triples = match self
            .insert_knowledge_collection(
                operation_id,
                publish_operation_id,
                &ual,
                pending_data.dataset(),
                &metadata,
            )
            .await
        {
            Ok(count) => count,
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    ual = %ual,
                    error = %e,
                    "Failed to insert Knowledge Collection to Triple Store"
                );
                return CommandExecutionResult::Completed;
            }
        };

        // Increment the total triples counter
        if let Err(e) = self
            .repository_manager
            .triples_insert_count_repository()
            .atomic_increment(total_triples as i64)
            .await
        {
            tracing::warn!(
                operation_id = %operation_id,
                total_triples = total_triples,
                error = %e,
                "Failed to increment triples count, continuing anyway"
            );
        } else {
            tracing::info!(
                operation_id = %operation_id,
                total_triples = total_triples,
                "Number of triples added to the database +{}", total_triples
            );
        }

        let publisher_peer_id: PeerId = match pending_data.publisher_peer_id().parse() {
            Ok(peer_id) => peer_id,
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    publisher_peer_id = %pending_data.publisher_peer_id(),
                    error = %e,
                    "Failed to parse publisher peer ID"
                );
                return CommandExecutionResult::Completed;
            }
        };

        if &publisher_peer_id == self.network_manager.peer_id() {
            tracing::debug!(
                operation_id = %operation_id,
                publish_operation_id = %publish_operation_id,
                ual = %ual,
                "Saving finality ack"
            );
            // Save the finality ack to the database
            if let Err(e) = self
                .repository_manager
                .finality_status_repository()
                .save_finality_ack(publish_operation_id, &ual, &publisher_peer_id.to_base58())
                .await
            {
                tracing::error!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    ual = %ual,
                    error = %e,
                    "Failed to save finality ack"
                );
            }

            return CommandExecutionResult::Completed;
        }

        let message = RequestMessage {
            header: RequestMessageHeader {
                operation_id,
                message_type: RequestMessageType::ProtocolRequest,
            },
            data: FinalityRequestData::new(ual, data.publish_operation_id.clone()),
        };

        if let Err(e) = self
            .network_manager
            .send_protocol_request(ProtocolRequest::Finality {
                peer: publisher_peer_id,
                message,
            })
            .await
        {
            tracing::error!(
                operation_id = %operation_id,
                publish_operation_id = %publish_operation_id,
                peer = %publisher_peer_id,
                error = %e,
                "Failed to send finality request to publisher"
            );
        } else {
            tracing::info!(
                operation_id = %operation_id,
                publish_operation_id = %publish_operation_id,
                peer = %publisher_peer_id,
                "Sent finality request to publisher"
            );
        }

        CommandExecutionResult::Completed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Basic tests for extract_subject and group_nquads_by_subject are in
    // triple_store::rdf module. These tests focus on the business logic:
    // - Private-hash triple separation
    // - UAL generation
    // - Private-to-public subject matching

    #[test]
    fn test_sha256_hex() {
        // Verify against known hash
        let input = "http://example.org/private-subject";
        let hash = blockchain::utils::sha256_hex(input.as_bytes());
        assert_eq!(
            hash,
            "463d1eef8a0ee03ec64f40b0339790ba998d0bbd51d813afeb338e39b56b3dd3"
        );
    }

    #[test]
    fn test_sha256_hex_hidden_asset() {
        let input = "http://example.org/hidden-asset";
        let hash = blockchain::utils::sha256_hex(input.as_bytes());
        assert_eq!(
            hash,
            "ae7b1b4b814077753da582ed5bc9865a7a7c6da59984e0a9427ac63e93756d29"
        );
    }

    /// Test Case 1: Simple public-only dataset
    /// Generated from JS
    #[test]
    fn test_simple_public_only() {
        let kc_ual = "did:dkg:hardhat1:31337/0x123/1";
        let public_triples = vec![
            r#"<http://example.org/subject1> <http://example.org/predicate1> "value1" ."#,
            r#"<http://example.org/subject1> <http://example.org/predicate2> "value2" ."#,
            r#"<http://example.org/subject2> <http://example.org/predicate1> "value3" ."#,
        ];

        let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);

        // Separate triples (no private hash triples in this case)
        let mut filtered_public: Vec<&str> = Vec::new();
        let mut private_hash_triples: Vec<&str> = Vec::new();

        for triple in &public_triples {
            if triple.starts_with(&private_hash_prefix) {
                private_hash_triples.push(triple);
            } else {
                filtered_public.push(triple);
            }
        }

        // Expected: all triples are filtered public
        assert_eq!(filtered_public.len(), 3);
        assert_eq!(private_hash_triples.len(), 0);

        // Group by subject
        let mut public_ka_grouped = group_nquads_by_subject(&filtered_public);
        public_ka_grouped.extend(group_nquads_by_subject(&private_hash_triples));

        // Expected: 2 groups
        assert_eq!(public_ka_grouped.len(), 2);
        assert_eq!(public_ka_grouped[0].len(), 2); // subject1
        assert_eq!(public_ka_grouped[1].len(), 1); // subject2

        // Generate UALs
        let public_ka_uals: Vec<String> = (0..public_ka_grouped.len())
            .map(|i| format!("{}/{}", kc_ual, i + 1))
            .collect();

        assert_eq!(
            public_ka_uals,
            vec![
                "did:dkg:hardhat1:31337/0x123/1/1",
                "did:dkg:hardhat1:31337/0x123/1/2"
            ]
        );
    }

    /// Test Case 2: Dataset with private hash triples in public
    /// Generated from JS
    #[test]
    fn test_with_private_hash_triples() {
        let kc_ual = "did:dkg:hardhat1:31337/0x456/2";
        let hashed_subject =
            blockchain::utils::sha256_hex("http://example.org/private-subject".as_bytes());
        let hash_triple = format!(
            r#"<{}{}> <http://example.org/predicate1> "hashed_value" ."#,
            PRIVATE_HASH_SUBJECT_PREFIX, hashed_subject
        );

        let public_triples: Vec<&str> = vec![
            r#"<http://example.org/subject1> <http://example.org/predicate1> "value1" ."#,
            &hash_triple,
        ];

        let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);

        let mut filtered_public: Vec<&str> = Vec::new();
        let mut private_hash_triples: Vec<&str> = Vec::new();

        for triple in &public_triples {
            if triple.starts_with(&private_hash_prefix) {
                private_hash_triples.push(triple);
            } else {
                filtered_public.push(triple);
            }
        }

        // Expected: 1 regular, 1 private hash
        assert_eq!(filtered_public.len(), 1);
        assert_eq!(private_hash_triples.len(), 1);

        // Group and combine
        let mut public_ka_grouped = group_nquads_by_subject(&filtered_public);
        public_ka_grouped.extend(group_nquads_by_subject(&private_hash_triples));

        // Expected: 2 groups (regular subject + hash subject)
        assert_eq!(public_ka_grouped.len(), 2);

        let public_ka_uals: Vec<String> = (0..public_ka_grouped.len())
            .map(|i| format!("{}/{}", kc_ual, i + 1))
            .collect();

        assert_eq!(
            public_ka_uals,
            vec![
                "did:dkg:hardhat1:31337/0x456/2/1",
                "did:dkg:hardhat1:31337/0x456/2/2"
            ]
        );
    }

    /// Test Case 3: Public and private with direct subject match
    /// Generated from JS
    #[test]
    fn test_public_and_private_direct_match() {
        let kc_ual = "did:dkg:hardhat1:31337/0x789/3";

        let public_triples: Vec<&str> = vec![
            r#"<http://example.org/asset1> <http://example.org/name> "Asset One" ."#,
            r#"<http://example.org/asset1> <http://example.org/type> "Document" ."#,
            r#"<http://example.org/asset2> <http://example.org/name> "Asset Two" ."#,
        ];

        let private_triples: Vec<&str> = vec![
            r#"<http://example.org/asset1> <http://example.org/secret> "private data" ."#,
            r#"<http://example.org/asset2> <http://example.org/secret> "more private" ."#,
        ];

        let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);

        // Process public
        let mut filtered_public: Vec<&str> = Vec::new();
        let mut private_hash_triples_pub: Vec<&str> = Vec::new();

        for triple in &public_triples {
            if triple.starts_with(&private_hash_prefix) {
                private_hash_triples_pub.push(triple);
            } else {
                filtered_public.push(triple);
            }
        }

        let mut public_ka_grouped = group_nquads_by_subject(&filtered_public);
        public_ka_grouped.extend(group_nquads_by_subject(&private_hash_triples_pub));

        let public_ka_uals: Vec<String> = (0..public_ka_grouped.len())
            .map(|i| format!("{}/{}", kc_ual, i + 1))
            .collect();

        // Expected: 2 public KAs
        assert_eq!(
            public_ka_uals,
            vec![
                "did:dkg:hardhat1:31337/0x789/3/1",
                "did:dkg:hardhat1:31337/0x789/3/2"
            ]
        );

        // Process private - match to public by subject
        let private_ka_grouped = group_nquads_by_subject(&private_triples);

        let public_subject_map: HashMap<&str, usize> = public_ka_grouped
            .iter()
            .enumerate()
            .filter_map(|(idx, group)| {
                group
                    .first()
                    .and_then(|triple| extract_subject(triple).map(|subj| (subj, idx)))
            })
            .collect();

        let mut private_ka_uals: Vec<Option<&String>> = Vec::new();

        for private_group in &private_ka_grouped {
            if let Some(first_triple) = private_group.first() {
                if let Some(private_subject) = extract_subject(first_triple) {
                    if let Some(&idx) = public_subject_map.get(private_subject) {
                        private_ka_uals.push(Some(&public_ka_uals[idx]));
                    } else {
                        private_ka_uals.push(None);
                    }
                } else {
                    private_ka_uals.push(None);
                }
            }
        }

        // Expected: both private groups match public KAs
        assert_eq!(private_ka_uals.len(), 2);
        assert_eq!(
            private_ka_uals[0].map(|s| s.as_str()),
            Some("did:dkg:hardhat1:31337/0x789/3/1")
        );
        assert_eq!(
            private_ka_uals[1].map(|s| s.as_str()),
            Some("did:dkg:hardhat1:31337/0x789/3/2")
        );
    }

    /// Test Case 4: Private with hashed subject match
    /// Generated from JS
    #[test]
    fn test_private_with_hashed_subject_match() {
        let kc_ual = "did:dkg:hardhat1:31337/0xabc/4";
        let private_subject = "http://example.org/hidden-asset";
        let private_subject_hash = blockchain::utils::sha256_hex(private_subject.as_bytes());

        let hash_triple = format!(
            r#"<{}{}> <http://example.org/hash> "hash_placeholder" ."#,
            PRIVATE_HASH_SUBJECT_PREFIX, private_subject_hash
        );

        let public_triples: Vec<&str> = vec![
            r#"<http://example.org/public-asset> <http://example.org/name> "Public" ."#,
            &hash_triple,
        ];

        let private_triple = format!(
            r#"<{}> <http://example.org/secret> "hidden data" ."#,
            private_subject
        );
        let private_triples: Vec<&str> = vec![&private_triple];

        let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);

        // Process public
        let mut filtered_public: Vec<&str> = Vec::new();
        let mut private_hash_triples_pub: Vec<&str> = Vec::new();

        for triple in &public_triples {
            if triple.starts_with(&private_hash_prefix) {
                private_hash_triples_pub.push(triple);
            } else {
                filtered_public.push(triple);
            }
        }

        let mut public_ka_grouped = group_nquads_by_subject(&filtered_public);
        public_ka_grouped.extend(group_nquads_by_subject(&private_hash_triples_pub));

        let public_ka_uals: Vec<String> = (0..public_ka_grouped.len())
            .map(|i| format!("{}/{}", kc_ual, i + 1))
            .collect();

        // Expected: 2 public KAs (public-asset and hash placeholder)
        assert_eq!(public_ka_uals.len(), 2);

        // Process private with hashed subject matching
        let private_ka_grouped = group_nquads_by_subject(&private_triples);

        let public_subject_map: HashMap<&str, usize> = public_ka_grouped
            .iter()
            .enumerate()
            .filter_map(|(idx, group)| {
                group
                    .first()
                    .and_then(|triple| extract_subject(triple).map(|subj| (subj, idx)))
            })
            .collect();

        let mut private_ka_uals: Vec<Option<&String>> = Vec::new();

        for private_group in &private_ka_grouped {
            if let Some(first_triple) = private_group.first() {
                if let Some(priv_subject) = extract_subject(first_triple) {
                    // Try direct match first
                    if let Some(&idx) = public_subject_map.get(priv_subject) {
                        private_ka_uals.push(Some(&public_ka_uals[idx]));
                    } else {
                        // Try hashed match
                        let subject_without_brackets =
                            priv_subject.trim_start_matches('<').trim_end_matches('>');
                        let hashed_subject = format!(
                            "<{}{}>",
                            PRIVATE_HASH_SUBJECT_PREFIX,
                            blockchain::utils::sha256_hex(subject_without_brackets.as_bytes())
                        );

                        if let Some(&idx) = public_subject_map.get(hashed_subject.as_str()) {
                            private_ka_uals.push(Some(&public_ka_uals[idx]));
                        } else {
                            private_ka_uals.push(None);
                        }
                    }
                } else {
                    private_ka_uals.push(None);
                }
            }
        }

        // Expected: private matches to the second public KA (the hash placeholder)
        assert_eq!(private_ka_uals.len(), 1);
        assert_eq!(
            private_ka_uals[0].map(|s| s.as_str()),
            Some("did:dkg:hardhat1:31337/0xabc/4/2")
        );
    }

    /// Test Case 5: Multiple subjects with various triples
    /// Generated from JS
    #[test]
    fn test_multiple_subjects_complex() {
        let kc_ual = "did:dkg:otp:2043/0xdef/5";

        let public_triples: Vec<&str> = vec![
            r#"<http://example.org/person/1> <http://schema.org/name> "Alice" ."#,
            r#"<http://example.org/person/1> <http://schema.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> ."#,
            r#"<http://example.org/person/2> <http://schema.org/name> "Bob" ."#,
            r#"<http://example.org/organization/1> <http://schema.org/name> "Acme Corp" ."#,
            r#"<http://example.org/person/1> <http://schema.org/worksFor> <http://example.org/organization/1> ."#,
        ];

        let private_triples: Vec<&str> = vec![
            r#"<http://example.org/person/1> <http://example.org/ssn> "123-45-6789" ."#,
            r#"<http://example.org/person/2> <http://example.org/ssn> "987-65-4321" ."#,
        ];

        let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);

        // Process public
        let mut filtered_public: Vec<&str> = Vec::new();
        let mut private_hash_triples_pub: Vec<&str> = Vec::new();

        for triple in &public_triples {
            if triple.starts_with(&private_hash_prefix) {
                private_hash_triples_pub.push(triple);
            } else {
                filtered_public.push(triple);
            }
        }

        let mut public_ka_grouped = group_nquads_by_subject(&filtered_public);
        public_ka_grouped.extend(group_nquads_by_subject(&private_hash_triples_pub));

        // Expected: 3 groups (person/1, person/2, organization/1)
        assert_eq!(public_ka_grouped.len(), 3);
        assert_eq!(public_ka_grouped[0].len(), 3); // person/1 has 3 triples
        assert_eq!(public_ka_grouped[1].len(), 1); // person/2 has 1 triple
        assert_eq!(public_ka_grouped[2].len(), 1); // organization/1 has 1 triple

        let public_ka_uals: Vec<String> = (0..public_ka_grouped.len())
            .map(|i| format!("{}/{}", kc_ual, i + 1))
            .collect();

        assert_eq!(
            public_ka_uals,
            vec![
                "did:dkg:otp:2043/0xdef/5/1",
                "did:dkg:otp:2043/0xdef/5/2",
                "did:dkg:otp:2043/0xdef/5/3"
            ]
        );

        // Process private
        let private_ka_grouped = group_nquads_by_subject(&private_triples);

        let public_subject_map: HashMap<&str, usize> = public_ka_grouped
            .iter()
            .enumerate()
            .filter_map(|(idx, group)| {
                group
                    .first()
                    .and_then(|triple| extract_subject(triple).map(|subj| (subj, idx)))
            })
            .collect();

        let mut private_ka_uals: Vec<Option<&String>> = Vec::new();

        for private_group in &private_ka_grouped {
            if let Some(first_triple) = private_group.first() {
                if let Some(private_subject) = extract_subject(first_triple) {
                    if let Some(&idx) = public_subject_map.get(private_subject) {
                        private_ka_uals.push(Some(&public_ka_uals[idx]));
                    } else {
                        private_ka_uals.push(None);
                    }
                } else {
                    private_ka_uals.push(None);
                }
            }
        }

        // Expected: both private groups match (person/1 -> KA1, person/2 -> KA2)
        assert_eq!(private_ka_uals.len(), 2);
        assert_eq!(
            private_ka_uals[0].map(|s| s.as_str()),
            Some("did:dkg:otp:2043/0xdef/5/1")
        );
        assert_eq!(
            private_ka_uals[1].map(|s| s.as_str()),
            Some("did:dkg:otp:2043/0xdef/5/2")
        );
    }
}
