use std::sync::Arc;

use dkg_domain::Visibility;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    application::{GetAssertionInput, GetOperationWorkflow},
    commands::SendGetRequestsDeps,
    commands::{executor::CommandOutcome, registry::CommandHandler},
};

/// Command data for sending get requests to network nodes.
#[derive(Clone)]
pub(crate) struct SendGetRequestsCommandData {
    pub operation_id: Uuid,
    pub ual: String,
    pub include_metadata: bool,
    pub paranet_ual: Option<String>,
    pub visibility: Visibility,
}

impl SendGetRequestsCommandData {
    pub(crate) fn new(
        operation_id: Uuid,
        ual: String,
        include_metadata: bool,
        paranet_ual: Option<String>,
        visibility: Visibility,
    ) -> Self {
        Self {
            operation_id,
            ual,
            include_metadata,
            paranet_ual,
            visibility,
        }
    }
}

pub(crate) struct SendGetRequestsCommandHandler {
    get_operation_workflow: Arc<GetOperationWorkflow>,
}

impl SendGetRequestsCommandHandler {
    pub(crate) fn new(deps: SendGetRequestsDeps) -> Self {
        Self {
            get_operation_workflow: deps.get_operation_workflow,
        }
    }
}

impl CommandHandler<SendGetRequestsCommandData> for SendGetRequestsCommandHandler {
    #[instrument(
        name = "op.get.send",
        skip(self, data),
        fields(
            operation_id = %data.operation_id,
            protocol = "get",
            direction = "send",
            ual = %data.ual,
            paranet = ?data.paranet_ual,
            visibility = ?data.visibility,
            include_metadata = data.include_metadata,
        )
    )]
    async fn execute(&self, data: &SendGetRequestsCommandData) -> CommandOutcome {
        let input = GetAssertionInput {
            operation_id: data.operation_id,
            ual: data.ual.clone(),
            include_metadata: data.include_metadata,
            paranet_ual: data.paranet_ual.clone(),
            visibility: data.visibility,
        };

        self.get_operation_workflow.execute(&input).await;

        CommandOutcome::Completed
    }
}
