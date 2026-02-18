use std::sync::Arc;

use dkg_blockchain::{Address, B256, BlockchainId, U256};
use tracing::instrument;

use crate::{
    application::{PublishFinalityInput, PublishFinalityWorkflow},
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
    /// The transaction hash (used to fetch publisher address)
    pub transaction_hash: B256,
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
        dataset_root: B256,
        transaction_hash: B256,
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
    publish_finality_workflow: Arc<PublishFinalityWorkflow>,
}

impl SendPublishFinalityRequestCommandHandler {
    pub(crate) fn new(deps: SendPublishFinalityRequestDeps) -> Self {
        Self {
            publish_finality_workflow: deps.publish_finality_workflow,
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
            protocol = "publish_finality",
            direction = "send",
            publish_operation_id = %data.publish_operation_id,
            blockchain = %data.blockchain,
            kc_id = %data.knowledge_collection_id,
            block_number = data.block_number,
        )
    )]
    async fn execute(&self, data: &SendPublishFinalityRequestCommandData) -> CommandOutcome {
        let input = PublishFinalityInput {
            blockchain: data.blockchain.clone(),
            publish_operation_id: data.publish_operation_id.clone(),
            knowledge_collection_id: data.knowledge_collection_id,
            knowledge_collection_storage_address: data.knowledge_collection_storage_address,
            byte_size: data.byte_size,
            dataset_root: data.dataset_root,
            transaction_hash: data.transaction_hash,
            block_number: data.block_number,
            block_timestamp: data.block_timestamp,
        };

        self.publish_finality_workflow.execute(&input).await;

        CommandOutcome::Completed
    }
}
