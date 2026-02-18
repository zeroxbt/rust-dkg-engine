use std::sync::Arc;

use dkg_blockchain::{Address, B256, BlockchainId, BlockchainManager, U256};
use dkg_domain::{KnowledgeCollectionMetadata, derive_ual};
use dkg_key_value_store::PublishTmpDatasetStore;
use dkg_network::{
    FinalityRequestData, FinalityResponseData, NetworkManager, PeerId, STREAM_PROTOCOL_FINALITY,
};
use dkg_repository::{FinalityStatusRepository, TriplesInsertCountRepository};
use uuid::Uuid;

use crate::{application::TripleStoreAssertions, node_state::PeerRegistry};

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
        let operation_id = Uuid::new_v4();

        let publish_operation_id = match Uuid::parse_str(&input.publish_operation_id) {
            Ok(uuid) => uuid,
            Err(e) => {
                tracing::error!(
                    publish_operation_id = %input.publish_operation_id,
                    error = %e,
                    "Failed to parse publish_operation_id as UUID"
                );
                return;
            }
        };

        let Ok(knowledge_collection_id) = input.knowledge_collection_id.try_into() else {
            tracing::error!(
            knowledge_collection_id = %input.knowledge_collection_id,
            "Knowledge collection ID exceeds u128 max"
            );
            return;
        };

        let pending_data = match self
            .publish_tmp_dataset_store
            .get(publish_operation_id)
            .await
        {
            Ok(Some(data)) => data,
            Ok(None) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    "Dataset not in publish tmp dataset store, skipping finality"
                );
                return;
            }
            Err(e) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    error = %e,
                    "Failed to read publish tmp dataset store, skipping finality"
                );
                return;
            }
        };

        let publisher_address = match self
            .blockchain_manager
            .get_transaction_sender(&input.blockchain, input.transaction_hash)
            .await
        {
            Ok(Some(addr)) => addr,
            Ok(None) => {
                tracing::error!(
                    tx_hash = %input.transaction_hash,
                    "Transaction not found, cannot determine publisher address"
                );
                return;
            }
            Err(e) => {
                tracing::error!(
                    tx_hash = %input.transaction_hash,
                    error = %e,
                    "Failed to fetch transaction"
                );
                return;
            }
        };

        tracing::debug!(
            operation_id = %operation_id,
            blockchain = %input.blockchain,
            knowledge_collection_id = knowledge_collection_id,
            byte_size = input.byte_size,
            publisher = %publisher_address,
            block_number = input.block_number,
            "Processing publish finality event"
        );

        let blockchain_merkle_root =
            format!("0x{}", dkg_blockchain::to_hex_string(input.dataset_root));
        if blockchain_merkle_root != pending_data.dataset_root() {
            tracing::error!(
                operation_id = %operation_id,
                blockchain_merkle_root = %blockchain_merkle_root,
                cached_merkle_root = %pending_data.dataset_root(),
                "Merkle root mismatch: blockchain value does not match cached value"
            );
            return;
        }

        let calculated_size = dkg_domain::calculate_assertion_size(&pending_data.dataset().public);
        if input.byte_size != calculated_size as u128 {
            tracing::error!(
                operation_id = %operation_id,
                blockchain_byte_size = input.byte_size,
                calculated_byte_size = calculated_size,
                "Byte size mismatch: blockchain value does not match calculated value"
            );
            return;
        }

        tracing::debug!(
            operation_id = %operation_id,
            "Publish data validation successful"
        );

        let metadata = KnowledgeCollectionMetadata::new(
            publisher_address.to_string().to_lowercase(),
            input.block_number,
            input.transaction_hash.to_string(),
            input.block_timestamp,
        );

        let ual = derive_ual(
            &input.blockchain,
            &input.knowledge_collection_storage_address,
            knowledge_collection_id,
            None,
        );

        tracing::debug!(
            operation_id = %operation_id,
            publish_operation_id = %publish_operation_id,
            ual = %ual,
            "Inserting knowledge collection into triple store"
        );

        let total_triples = match self
            .triple_store_assertions
            .insert_knowledge_collection(&ual, pending_data.dataset(), &Some(metadata), None)
            .await
        {
            Ok(count) => {
                tracing::info!(
                    operation_id = %operation_id,
                    ual = %ual,
                    total_triples = count,
                    "Knowledge collection inserted into triple store"
                );
                count
            }
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    ual = %ual,
                    error = %e,
                    "Failed to insert Knowledge Collection to Triple Store"
                );
                return;
            }
        };

        if let Err(e) = self
            .publish_tmp_dataset_store
            .remove(publish_operation_id)
            .await
        {
            tracing::warn!(
                operation_id = %operation_id,
                publish_operation_id = %publish_operation_id,
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
                operation_id = %operation_id,
                total_triples = total_triples,
                error = %e,
                "Failed to increment triples count, continuing anyway"
            );
        } else {
            tracing::debug!(
                operation_id = %operation_id,
                total_triples = total_triples,
                "Triples counter incremented"
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
                return;
            }
        };

        if &publisher_peer_id == self.network_manager.peer_id() {
            tracing::debug!(
                operation_id = %operation_id,
                publish_operation_id = %publish_operation_id,
                ual = %ual,
                "Saving finality ack"
            );

            if let Err(e) = self
                .finality_status_repository
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

            return;
        }

        if !self
            .peer_registry
            .peer_supports_protocol(&publisher_peer_id, STREAM_PROTOCOL_FINALITY)
        {
            tracing::warn!(
                operation_id = %operation_id,
                publish_operation_id = %publish_operation_id,
                peer = %publisher_peer_id,
                "Publisher does not advertise finality protocol; skipping request"
            );
            return;
        }

        let finality_request_data = FinalityRequestData::new(
            ual,
            input.publish_operation_id.clone(),
            input.blockchain.clone(),
        );
        let result = self
            .network_manager
            .send_finality_request(publisher_peer_id, operation_id, finality_request_data)
            .await;

        match result {
            Ok(FinalityResponseData::Ack(_)) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    peer = %publisher_peer_id,
                    "Finality request acknowledged by publisher"
                );
            }
            Ok(FinalityResponseData::Error(_)) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    peer = %publisher_peer_id,
                    "Publisher returned error for finality request"
                );
            }
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    peer = %publisher_peer_id,
                    error = %e,
                    "Failed to send finality request to publisher"
                );
            }
        }
    }
}
