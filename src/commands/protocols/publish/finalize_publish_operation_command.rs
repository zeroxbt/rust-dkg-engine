use std::sync::Arc;

use blockchain::{Address, BlockchainId, BlockchainManager, H256, U256};
use network::NetworkManager;
use repository::RepositoryManager;
use uuid::Uuid;

use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    network::NetworkProtocols,
    services::operation_manager::OperationManager,
};

/// Raw event data from KnowledgeCollectionCreated event.
/// Parsing and validation happens in the command handler, not the event listener.
#[derive(Clone)]
pub struct FinalizePublishOperationCommandData {
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

impl FinalizePublishOperationCommandData {
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

pub struct FinalizePublishOperationCommandHandler {
    repository_manager: Arc<RepositoryManager>,
    network_manager: Arc<NetworkManager<NetworkProtocols>>,
    blockchain_manager: Arc<BlockchainManager>,
    publish_operation_manager: Arc<OperationManager>,
}

impl FinalizePublishOperationCommandHandler {
    pub fn new(context: Arc<Context>) -> Self {
        Self {
            repository_manager: Arc::clone(context.repository_manager()),
            network_manager: Arc::clone(context.network_manager()),
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            publish_operation_manager: Arc::clone(context.publish_operation_manager()),
        }
    }
}

impl CommandHandler<FinalizePublishOperationCommandData>
    for FinalizePublishOperationCommandHandler
{
    async fn execute(&self, data: &FinalizePublishOperationCommandData) -> CommandExecutionResult {
        // Parse the operation ID from the raw string
        let operation_id = match Uuid::parse_str(&data.publish_operation_id) {
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

        // TODO: Implement the actual finalization logic
        // - Look up the pending publish operation
        // - Send finality messages to nodes
        // - Update operation status

        CommandExecutionResult::Completed
    }
}
