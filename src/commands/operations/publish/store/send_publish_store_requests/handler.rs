use std::sync::Arc;

use dkg_domain::Assertion;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    application::{ExecutePublishStoreInput, ExecutePublishStoreWorkflow},
    commands::SendPublishStoreRequestsDeps,
    commands::{executor::CommandOutcome, registry::CommandHandler},
};

/// Command data for sending publish store requests to network nodes.
/// Dataset is passed inline instead of being retrieved from storage.
#[derive(Clone)]
pub(crate) struct SendPublishStoreRequestsCommandData {
    pub operation_id: Uuid,
    pub blockchain: dkg_blockchain::BlockchainId,
    pub dataset_root: String,
    pub min_ack_responses: u8,
    pub dataset: Assertion,
}

impl SendPublishStoreRequestsCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        blockchain: dkg_blockchain::BlockchainId,
        dataset_root: String,
        min_ack_responses: u8,
        dataset: Assertion,
    ) -> Self {
        Self {
            operation_id,
            blockchain,
            dataset_root,
            min_ack_responses,
            dataset,
        }
    }
}

pub(crate) struct SendPublishStoreRequestsCommandHandler {
    execute_publish_store_workflow: Arc<ExecutePublishStoreWorkflow>,
}

impl SendPublishStoreRequestsCommandHandler {
    pub(crate) fn new(deps: SendPublishStoreRequestsDeps) -> Self {
        Self {
            execute_publish_store_workflow: deps.execute_publish_store_workflow,
        }
    }
}

impl CommandHandler<SendPublishStoreRequestsCommandData>
    for SendPublishStoreRequestsCommandHandler
{
    #[instrument(
        name = "op.publish_store.send",
        skip(self, data),
        fields(
            operation_id = %data.operation_id,
            protocol = "publish_store",
            direction = "send",
            blockchain = %data.blockchain,
            dataset_root = %data.dataset_root,
            min_ack_responses = data.min_ack_responses,
            dataset_public_len = data.dataset.public.len(),
        )
    )]
    async fn execute(&self, data: &SendPublishStoreRequestsCommandData) -> CommandOutcome {
        let input = ExecutePublishStoreInput {
            operation_id: data.operation_id,
            blockchain: data.blockchain.clone(),
            dataset_root: data.dataset_root.clone(),
            min_ack_responses: data.min_ack_responses,
            dataset: data.dataset.clone(),
        };

        self.execute_publish_store_workflow.execute(&input).await;

        CommandOutcome::Completed
    }
}
