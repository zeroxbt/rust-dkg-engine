use std::sync::Arc;

use dkg_blockchain::{Address, BlockchainId, BlockchainManager, H256, U256};
use dkg_domain::{KnowledgeCollectionMetadata, derive_ual};
use dkg_key_value_store::PublishTmpDatasetStore;
use dkg_network::{
    NetworkManager, PeerId, STREAM_PROTOCOL_FINALITY,
    messages::{FinalityRequestData, FinalityResponseData},
};
use dkg_repository::RepositoryManager;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    commands::{executor::CommandOutcome, registry::CommandHandler},
    context::Context,
    services::{PeerService, TripleStoreService},
};

/// Raw event data from KnowledgeCollectionCreated event.
/// Parsing and validation happens in the command handler, not the event listener.
#[derive(Clone)]
pub(crate) struct SendPublishFinalityRequestCommandData {
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

impl SendPublishFinalityRequestCommandData {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
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

pub(crate) struct SendPublishFinalityRequestCommandHandler {
    pub(super) repository_manager: Arc<RepositoryManager>,
    pub(super) network_manager: Arc<NetworkManager>,
    peer_service: Arc<PeerService>,
    blockchain_manager: Arc<BlockchainManager>,
    publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    triple_store_service: Arc<TripleStoreService>,
}

impl SendPublishFinalityRequestCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            peer_service: Arc::clone(context.peer_service()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            publish_tmp_dataset_store: Arc::clone(context.publish_tmp_dataset_store()),
            triple_store_service: Arc::clone(context.triple_store_service()),
        }
    }
}

impl CommandHandler<SendPublishFinalityRequestCommandData>
    for SendPublishFinalityRequestCommandHandler
{
    #[instrument(
        name = "op.publish_finality.send",
        skip(self, data),
        fields(
            operation_id = tracing::field::Empty,
            protocol = "publish_finality",
            direction = "send",
            publish_operation_id = %data.publish_operation_id,
            blockchain = %data.blockchain,
            kc_id = %data.knowledge_collection_id,
            block_number = data.block_number,
        )
    )]
    async fn execute(&self, data: &SendPublishFinalityRequestCommandData) -> CommandOutcome {
        // Generate a new operation ID for the finality request
        let operation_id = Uuid::new_v4();
        tracing::Span::current().record("operation_id", tracing::field::display(operation_id));
        // Parse the operation ID from the raw string
        let publish_operation_id = match Uuid::parse_str(&data.publish_operation_id) {
            Ok(uuid) => uuid,
            Err(e) => {
                tracing::error!(
                    publish_operation_id = %data.publish_operation_id,
                    error = %e,
                    "Failed to parse publish_operation_id as UUID"
                );
                return CommandOutcome::Completed;
            }
        };

        // Convert knowledge_collection_id from U256 to u128
        let Ok(knowledge_collection_id) = data.knowledge_collection_id.try_into() else {
            tracing::error!(
            knowledge_collection_id = %data.knowledge_collection_id,
            "Knowledge collection ID exceeds u128 max"
            );
            return CommandOutcome::Completed;
        };

        // Retrieve cached dataset from publish tmp dataset store
        // This will fail if this node doesn't have the dataset locally
        // (e.g., KC was published by another node, node was offline, or wasn't contacted during
        // publish)
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
                return CommandOutcome::Completed;
            }
            Err(e) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    error = %e,
                    "Failed to read publish tmp dataset store, skipping finality"
                );
                return CommandOutcome::Completed;
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
                return CommandOutcome::Completed;
            }
            Err(e) => {
                tracing::error!(
                    tx_hash = %data.transaction_hash,
                    error = %e,
                    "Failed to fetch transaction"
                );
                return CommandOutcome::Completed;
            }
        };

        tracing::debug!(
            operation_id = %operation_id,
            blockchain = %data.blockchain,
            knowledge_collection_id = knowledge_collection_id,
            byte_size = data.byte_size,
            publisher = %publisher_address,
            block_number = data.block_number,
            "Processing publish finality event"
        );

        // Validate merkle root matches
        let blockchain_merkle_root =
            format!("0x{}", dkg_blockchain::to_hex_string(data.dataset_root));
        if blockchain_merkle_root != pending_data.dataset_root() {
            tracing::error!(
                operation_id = %operation_id,
                blockchain_merkle_root = %blockchain_merkle_root,
                cached_merkle_root = %pending_data.dataset_root(),
                "Merkle root mismatch: blockchain value does not match cached value"
            );
            return CommandOutcome::Completed;
        }

        // Validate byte size matches
        let calculated_size = dkg_domain::calculate_assertion_size(&pending_data.dataset().public);
        if data.byte_size != calculated_size as u128 {
            tracing::error!(
                operation_id = %operation_id,
                blockchain_byte_size = data.byte_size,
                calculated_byte_size = calculated_size,
                "Byte size mismatch: blockchain value does not match calculated value"
            );
            return CommandOutcome::Completed;
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

        tracing::debug!(
            operation_id = %operation_id,
            publish_operation_id = %publish_operation_id,
            ual = %ual,
            "Inserting knowledge collection into triple store"
        );

        let total_triples = match self
            .triple_store_service
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
                return CommandOutcome::Completed;
            }
        };

        // Remove from publish tmp dataset store now that insertion succeeded
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
                return CommandOutcome::Completed;
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

            return CommandOutcome::Completed;
        }

        if !self
            .peer_service
            .peer_supports_protocol(&publisher_peer_id, STREAM_PROTOCOL_FINALITY)
        {
            tracing::warn!(
                operation_id = %operation_id,
                publish_operation_id = %publish_operation_id,
                peer = %publisher_peer_id,
                "Publisher does not advertise finality protocol; skipping request"
            );
            return CommandOutcome::Completed;
        }

        let finality_request_data = FinalityRequestData::new(
            ual,
            data.publish_operation_id.clone(),
            data.blockchain.clone(),
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

        CommandOutcome::Completed
    }
}
