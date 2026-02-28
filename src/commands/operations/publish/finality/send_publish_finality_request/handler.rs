use std::sync::Arc;

use dkg_blockchain::{Address, B256, BlockchainId, U256};
use dkg_domain::{KnowledgeCollectionMetadata, derive_ual};
use dkg_key_value_store::PublishTmpDatasetStore;
use dkg_network::{FinalityRequestData, FinalityResponseData, NetworkManager, PeerId};
use dkg_observability as observability;
use dkg_repository::{FinalityStatusRepository, OperationRepository, TriplesInsertCountRepository};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    application::TripleStoreAssertions,
    commands::SendPublishFinalityRequestDeps,
    commands::{executor::CommandOutcome, registry::CommandHandler},
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
    pub dataset_root: B256,
    /// Core chain metadata resolved by the listener.
    pub metadata: KnowledgeCollectionMetadata,
}

impl SendPublishFinalityRequestCommandData {
    pub(crate) fn new(
        blockchain: BlockchainId,
        publish_operation_id: String,
        knowledge_collection_id: U256,
        knowledge_collection_storage_address: Address,
        byte_size: u128,
        dataset_root: B256,
        metadata: KnowledgeCollectionMetadata,
    ) -> Self {
        Self {
            blockchain,
            publish_operation_id,
            knowledge_collection_id,
            knowledge_collection_storage_address,
            byte_size,
            dataset_root,
            metadata,
        }
    }
}

pub(crate) struct SendPublishFinalityRequestCommandHandler {
    finality_status_repository: FinalityStatusRepository,
    operation_repository: OperationRepository,
    triples_insert_count_repository: TriplesInsertCountRepository,
    pub(super) network_manager: Arc<NetworkManager>,
    publish_tmp_dataset_store: Arc<PublishTmpDatasetStore>,
    triple_store_assertions: Arc<TripleStoreAssertions>,
}

impl SendPublishFinalityRequestCommandHandler {
    pub(crate) fn new(deps: SendPublishFinalityRequestDeps) -> Self {
        Self {
            finality_status_repository: deps.finality_status_repository,
            operation_repository: deps.operation_repository,
            triples_insert_count_repository: deps.triples_insert_count_repository,
            network_manager: deps.network_manager,
            publish_tmp_dataset_store: deps.publish_tmp_dataset_store,
            triple_store_assertions: deps.triple_store_assertions,
        }
    }

    async fn record_publish_finalization_metrics(
        &self,
        blockchain_id: &str,
        publish_operation_id: Uuid,
        finalization_block_timestamp_secs: u64,
    ) {
        observability::record_publish_finalization_total(blockchain_id);

        let created_at_ms = match self
            .operation_repository
            .get_created_at_timestamp_millis(publish_operation_id)
            .await
        {
            Ok(Some(created_at_ms)) => created_at_ms.max(0) as u64,
            Ok(None) => {
                tracing::warn!(
                    publish_operation_id = %publish_operation_id,
                    blockchain_id = blockchain_id,
                    "Publish operation missing while recording finalization duration"
                );
                return;
            }
            Err(error) => {
                tracing::warn!(
                    publish_operation_id = %publish_operation_id,
                    blockchain_id = blockchain_id,
                    error = %error,
                    "Failed to load publish operation start time for finalization duration"
                );
                return;
            }
        };

        let finalization_ms = finalization_block_timestamp_secs.saturating_mul(1000);
        let duration_ms = finalization_ms.saturating_sub(created_at_ms);

        observability::record_publish_finalization_duration(
            blockchain_id,
            std::time::Duration::from_millis(duration_ms),
        );
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
            block_number = data.metadata.block_number(),
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
                tracing::debug!("Dataset not in publish tmp dataset store, skipping finality");
                return CommandOutcome::Completed;
            }
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "Failed to read publish tmp dataset store, skipping finality"
                );
                return CommandOutcome::Completed;
            }
        };

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

        let metadata = data.metadata.clone();

        // Derive UAL for the knowledge collection
        let ual = derive_ual(
            &data.blockchain,
            &data.knowledge_collection_storage_address,
            knowledge_collection_id,
            None,
        );

        let total_triples = match self
            .triple_store_assertions
            .insert_knowledge_collection(&ual, pending_data.dataset(), &Some(metadata), None)
            .await
        {
            Ok(count) => {
                tracing::info!(
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
            self.record_publish_finalization_metrics(
                data.blockchain.as_str(),
                publish_operation_id,
                data.metadata.block_timestamp(),
            )
            .await;

            // Save the finality ack to the database
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
