use std::{ops::ControlFlow, sync::Arc};

use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::{Address, BlockchainId, BlockchainManager, H256, U256},
        network::{NetworkManager, PeerId, messages::FinalityRequestData},
        repository::RepositoryManager,
        triple_store::KnowledgeCollectionMetadata,
    },
    services::{TripleStoreService, pending_storage_service::PendingStorageService},
    types::derive_ual,
    utils::{peer_fanout::for_each_peer_concurrently, validation},
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
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager>,
    blockchain_manager: Arc<BlockchainManager>,
    pending_storage_service: Arc<PendingStorageService>,
    triple_store_service: Arc<TripleStoreService>,
}

impl SendPublishFinalityRequestCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            pending_storage_service: Arc::clone(context.pending_storage_service()),
            triple_store_service: Arc::clone(context.triple_store_service()),
        }
    }
}

impl CommandHandler<SendPublishFinalityRequestCommandData>
    for SendPublishFinalityRequestCommandHandler
{
    async fn execute(
        &self,
        data: &SendPublishFinalityRequestCommandData,
    ) -> CommandExecutionResult {
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
        // This will fail if this node doesn't have the dataset locally
        // (e.g., KC was published by another node, node was offline, or wasn't contacted during
        // publish)
        let pending_data = match self
            .pending_storage_service
            .get_dataset(publish_operation_id)
        {
            Ok(data) => data,
            Err(e) => {
                tracing::debug!(
                    operation_id = %operation_id,
                    publish_operation_id = %publish_operation_id,
                    error = %e,
                    "Dataset not in pending storage, skipping finality"
                );
                return CommandExecutionResult::Completed;
            }
        };

        // Validate merkle root matches
        let blockchain_merkle_root = format!(
            "0x{}",
            crate::managers::blockchain::utils::to_hex_string(data.dataset_root)
        );
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
        let calculated_size = validation::calculate_assertion_size(&pending_data.dataset().public);
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

        tracing::info!(
            operation_id = %operation_id,
            publish_operation_id = %publish_operation_id,
            ual = %ual,
            "Inserting Knowledge Collection to the Triple Store's dkg repository."
        );

        let total_triples = match self
            .triple_store_service
            .insert_knowledge_collection(&ual, pending_data.dataset(), &Some(metadata))
            .await
        {
            Ok(count) => {
                tracing::info!(
                    operation_id = %operation_id,
                    ual = %ual,
                    total_triples = count,
                    "Knowledge Collection has been successfully inserted to the Triple Store."
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
                return CommandExecutionResult::Completed;
            }
        };

        // Remove from pending storage now that insertion succeeded
        if let Err(e) = self.pending_storage_service.remove(publish_operation_id) {
            tracing::warn!(
                operation_id = %operation_id,
                publish_operation_id = %publish_operation_id,
                error = %e,
                "Failed to remove dataset from pending storage"
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

        let finality_request_data = FinalityRequestData::new(
            ual,
            data.publish_operation_id.clone(),
            data.blockchain.clone(),
        );

        let peers = vec![publisher_peer_id];
        for_each_peer_concurrently(
            &peers,
            crate::operations::protocols::publish_finality::CONCURRENT_PEERS,
            |peer| {
                let request_data = finality_request_data.clone();
                let network_manager = Arc::clone(&self.network_manager);
                async move {
                    let addresses = network_manager
                        .get_peer_addresses(peer)
                        .await
                        .unwrap_or_default();
                    let result = network_manager
                        .send_finality_request(peer, addresses, operation_id, request_data)
                        .await;
                    (peer, result)
                }
            },
            |(peer, result)| {
                match result {
                    Ok(_) => {
                        tracing::info!(
                            operation_id = %operation_id,
                            publish_operation_id = %publish_operation_id,
                            peer = %peer,
                            "Sent finality request to publisher"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            operation_id = %operation_id,
                            publish_operation_id = %publish_operation_id,
                            peer = %peer,
                            error = %e,
                            "Failed to send finality request to publisher"
                        );
                    }
                }

                ControlFlow::Break(())
            },
        )
        .await;

        CommandExecutionResult::Completed
    }
}
