use std::sync::Arc;

use dkg_blockchain::{Address, B256, BlockchainId, BlockchainManager, U256};
use dkg_domain::{KnowledgeCollectionMetadata, derive_ual};
use dkg_key_value_store::{PublishTmpDataset, PublishTmpDatasetStore};
use dkg_network::{
    FinalityRequestData, FinalityResponseData, NetworkManager, PeerId, STREAM_PROTOCOL_FINALITY,
};
use dkg_peer_registry::PeerRegistry;
use dkg_repository::{FinalityStatusRepository, TriplesInsertCountRepository};
use uuid::Uuid;

use crate::application::TripleStoreAssertions;

#[derive(Clone)]
pub(crate) struct ProcessPublishFinalityEventInput {
    pub blockchain: BlockchainId,
    pub publish_operation_id: String,
    pub knowledge_collection_id: U256,
    pub knowledge_collection_storage_address: Address,
    pub byte_size: u128,
    pub dataset_root: B256,
    pub transaction_hash: B256,
    pub block_number: u64,
    pub block_timestamp: u64,
}

struct FinalityProcessingContext {
    operation_id: Uuid,
    publish_operation_id: Uuid,
    knowledge_collection_id: u128,
    pending_data: PublishTmpDataset,
    publisher_address: Address,
    ual: String,
}

pub(crate) struct ProcessPublishFinalityEventWorkflow {
    finality_status_repository: FinalityStatusRepository,
    triples_insert_count_repository: TriplesInsertCountRepository,
    network_manager: Arc<NetworkManager>,
    peer_registry: Arc<PeerRegistry>,
    blockchain_manager: Arc<BlockchainManager>,
    publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    triple_store_assertions: Arc<TripleStoreAssertions>,
}

impl ProcessPublishFinalityEventWorkflow {
    pub(crate) fn new(
        finality_status_repository: FinalityStatusRepository,
        triples_insert_count_repository: TriplesInsertCountRepository,
        network_manager: Arc<NetworkManager>,
        peer_registry: Arc<PeerRegistry>,
        blockchain_manager: Arc<BlockchainManager>,
        publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
        triple_store_assertions: Arc<TripleStoreAssertions>,
    ) -> Self {
        Self {
            finality_status_repository,
            triples_insert_count_repository,
            network_manager,
            peer_registry,
            blockchain_manager,
            publish_tmp_dataset_store,
            triple_store_assertions,
        }
    }

    pub(crate) async fn execute(&self, input: &ProcessPublishFinalityEventInput) {
        let Some(context) = self.prepare_context(input).await else {
            return;
        };

        let Some(total_triples) = self.insert_knowledge_collection(input, &context).await else {
            return;
        };

        self.finalize_persistence(&context, total_triples).await;
        self.notify_publisher(input, &context).await;
    }

    async fn prepare_context(
        &self,
        input: &ProcessPublishFinalityEventInput,
    ) -> Option<FinalityProcessingContext> {
        let operation_id = Uuid::new_v4();
        let publish_operation_id = self.parse_publish_operation_id(operation_id, input)?;
        let knowledge_collection_id =
            self.parse_knowledge_collection_id(operation_id, input.knowledge_collection_id)?;
        let pending_data = self
            .load_pending_data(operation_id, publish_operation_id)
            .await?;
        let publisher_address = self.resolve_publisher_address(operation_id, input).await?;

        tracing::debug!(
            operation_id = %operation_id,
            blockchain = %input.blockchain,
            knowledge_collection_id = knowledge_collection_id,
            byte_size = input.byte_size,
            publisher = %publisher_address,
            block_number = input.block_number,
            "Processing publish finality event"
        );

        if !self.validate_publish_data(operation_id, input, &pending_data) {
            return None;
        }

        let ual = derive_ual(
            &input.blockchain,
            &input.knowledge_collection_storage_address,
            knowledge_collection_id,
            None,
        );

        Some(FinalityProcessingContext {
            operation_id,
            publish_operation_id,
            knowledge_collection_id,
            pending_data,
            publisher_address,
            ual,
        })
    }

    fn parse_publish_operation_id(
        &self,
        operation_id: Uuid,
        input: &ProcessPublishFinalityEventInput,
    ) -> Option<Uuid> {
        match Uuid::parse_str(&input.publish_operation_id) {
            Ok(uuid) => Some(uuid),
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    publish_operation_id = %input.publish_operation_id,
                    error = %e,
                    "Failed to parse publish_operation_id as UUID"
                );
                None
            }
        }
    }

    fn parse_knowledge_collection_id(
        &self,
        operation_id: Uuid,
        knowledge_collection_id: U256,
    ) -> Option<u128> {
        match knowledge_collection_id.try_into() {
            Ok(id) => Some(id),
            Err(_) => {
                tracing::error!(
                    operation_id = %operation_id,
                    knowledge_collection_id = %knowledge_collection_id,
                    "Knowledge collection ID exceeds u128 max"
                );
                None
            }
        }
    }

    async fn load_pending_data(
        &self,
        operation_id: Uuid,
        publish_operation_id: Uuid,
    ) -> Option<PublishTmpDataset> {
        match self
            .publish_tmp_dataset_store
            .get(publish_operation_id)
            .await
        {
            Ok(Some(data)) => Some(data),
            Ok(None) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    "Dataset not in publish tmp dataset store, skipping finality"
                );
                None
            }
            Err(e) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    error = %e,
                    "Failed to read publish tmp dataset store, skipping finality"
                );
                None
            }
        }
    }

    async fn resolve_publisher_address(
        &self,
        operation_id: Uuid,
        input: &ProcessPublishFinalityEventInput,
    ) -> Option<Address> {
        match self
            .blockchain_manager
            .get_transaction_sender(&input.blockchain, input.transaction_hash)
            .await
        {
            Ok(Some(addr)) => Some(addr),
            Ok(None) => {
                tracing::error!(
                    operation_id = %operation_id,
                    tx_hash = %input.transaction_hash,
                    "Transaction not found, cannot determine publisher address"
                );
                None
            }
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    tx_hash = %input.transaction_hash,
                    error = %e,
                    "Failed to fetch transaction"
                );
                None
            }
        }
    }

    fn validate_publish_data(
        &self,
        operation_id: Uuid,
        input: &ProcessPublishFinalityEventInput,
        pending_data: &PublishTmpDataset,
    ) -> bool {
        let blockchain_merkle_root =
            format!("0x{}", dkg_blockchain::to_hex_string(input.dataset_root));
        if blockchain_merkle_root != pending_data.dataset_root() {
            tracing::error!(
                operation_id = %operation_id,
                blockchain_merkle_root = %blockchain_merkle_root,
                cached_merkle_root = %pending_data.dataset_root(),
                "Merkle root mismatch: blockchain value does not match cached value"
            );
            return false;
        }

        let calculated_size = dkg_domain::calculate_assertion_size(&pending_data.dataset().public);
        if input.byte_size != calculated_size as u128 {
            tracing::error!(
                operation_id = %operation_id,
                blockchain_byte_size = input.byte_size,
                calculated_byte_size = calculated_size,
                "Byte size mismatch: blockchain value does not match calculated value"
            );
            return false;
        }

        tracing::debug!(
            operation_id = %operation_id,
            "Publish data validation successful"
        );
        true
    }

    async fn insert_knowledge_collection(
        &self,
        input: &ProcessPublishFinalityEventInput,
        context: &FinalityProcessingContext,
    ) -> Option<usize> {
        let metadata = KnowledgeCollectionMetadata::new(
            context.publisher_address.to_string().to_lowercase(),
            input.block_number,
            input.transaction_hash.to_string(),
            input.block_timestamp,
        );

        tracing::debug!(
            operation_id = %context.operation_id,
            publish_operation_id = %context.publish_operation_id,
            ual = %context.ual,
            knowledge_collection_id = context.knowledge_collection_id,
            "Inserting knowledge collection into triple store"
        );

        match self
            .triple_store_assertions
            .insert_knowledge_collection(
                &context.ual,
                context.pending_data.dataset(),
                &Some(metadata),
                None,
            )
            .await
        {
            Ok(count) => {
                tracing::info!(
                    operation_id = %context.operation_id,
                    ual = %context.ual,
                    total_triples = count,
                    "Knowledge collection inserted into triple store"
                );
                Some(count)
            }
            Err(e) => {
                tracing::error!(
                    operation_id = %context.operation_id,
                    ual = %context.ual,
                    error = %e,
                    "Failed to insert Knowledge Collection to Triple Store"
                );
                None
            }
        }
    }

    async fn finalize_persistence(
        &self,
        context: &FinalityProcessingContext,
        total_triples: usize,
    ) {
        if let Err(e) = self
            .publish_tmp_dataset_store
            .remove(context.publish_operation_id)
            .await
        {
            tracing::warn!(
                operation_id = %context.operation_id,
                publish_operation_id = %context.publish_operation_id,
                error = %e,
                "Failed to remove dataset from publish tmp dataset store"
            );
        }

        if let Err(e) = self
            .triples_insert_count_repository
            .atomic_increment(total_triples as i64)
            .await
        {
            tracing::warn!(
                operation_id = %context.operation_id,
                total_triples = total_triples,
                error = %e,
                "Failed to increment triples count, continuing anyway"
            );
            return;
        }

        tracing::debug!(
            operation_id = %context.operation_id,
            total_triples = total_triples,
            "Triples counter incremented"
        );
    }

    async fn notify_publisher(
        &self,
        input: &ProcessPublishFinalityEventInput,
        context: &FinalityProcessingContext,
    ) {
        let publisher_peer_id: PeerId = match context.pending_data.publisher_peer_id().parse() {
            Ok(peer_id) => peer_id,
            Err(e) => {
                tracing::error!(
                    operation_id = %context.operation_id,
                    publisher_peer_id = %context.pending_data.publisher_peer_id(),
                    error = %e,
                    "Failed to parse publisher peer ID"
                );
                return;
            }
        };

        if &publisher_peer_id == self.network_manager.peer_id() {
            self.save_local_finality_ack(context, &publisher_peer_id)
                .await;
            return;
        }

        if !self
            .peer_registry
            .peer_supports_protocol(&publisher_peer_id, STREAM_PROTOCOL_FINALITY)
        {
            tracing::warn!(
                operation_id = %context.operation_id,
                publish_operation_id = %context.publish_operation_id,
                peer = %publisher_peer_id,
                "Publisher does not advertise finality protocol; skipping request"
            );
            return;
        }

        self.send_finality_request(input, context, publisher_peer_id)
            .await;
    }

    async fn save_local_finality_ack(
        &self,
        context: &FinalityProcessingContext,
        publisher_peer_id: &PeerId,
    ) {
        tracing::debug!(
            operation_id = %context.operation_id,
            publish_operation_id = %context.publish_operation_id,
            ual = %context.ual,
            "Saving finality ack"
        );

        if let Err(e) = self
            .finality_status_repository
            .save_finality_ack(
                context.publish_operation_id,
                &context.ual,
                &publisher_peer_id.to_base58(),
            )
            .await
        {
            tracing::error!(
                operation_id = %context.operation_id,
                publish_operation_id = %context.publish_operation_id,
                ual = %context.ual,
                error = %e,
                "Failed to save finality ack"
            );
        }
    }

    async fn send_finality_request(
        &self,
        input: &ProcessPublishFinalityEventInput,
        context: &FinalityProcessingContext,
        publisher_peer_id: PeerId,
    ) {
        let finality_request_data = FinalityRequestData::new(
            context.ual.clone(),
            input.publish_operation_id.clone(),
            input.blockchain.clone(),
        );

        match self
            .network_manager
            .send_finality_request(
                publisher_peer_id,
                context.operation_id,
                finality_request_data,
            )
            .await
        {
            Ok(FinalityResponseData::Ack(_)) => {
                tracing::debug!(
                    operation_id = %context.operation_id,
                    publish_operation_id = %context.publish_operation_id,
                    peer = %publisher_peer_id,
                    "Finality request acknowledged by publisher"
                );
            }
            Ok(FinalityResponseData::Error(_)) => {
                tracing::warn!(
                    operation_id = %context.operation_id,
                    publish_operation_id = %context.publish_operation_id,
                    peer = %publisher_peer_id,
                    "Publisher returned error for finality request"
                );
            }
            Err(e) => {
                tracing::error!(
                    operation_id = %context.operation_id,
                    publish_operation_id = %context.publish_operation_id,
                    peer = %publisher_peer_id,
                    error = %e,
                    "Failed to send finality request to publisher"
                );
            }
        }
    }
}
