use std::sync::Arc;

use tracing::instrument;
use uuid::Uuid;

use crate::{
    commands::{executor::CommandOutcome, registry::CommandHandler},
    context::Context,
    operations::{GetOperation, GetOperationResult},
    services::{
        GetFetchRequest, operation_status::OperationStatusService as GenericOperationService,
    },
    types::Visibility,
};

/// Maximum number of in-flight peer requests for GET network fetch.
/// Kept here for compatibility with sync task imports.
pub(crate) const CONCURRENT_PEERS: usize = 3;

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
    pub(super) get_operation_status_service: Arc<GenericOperationService<GetOperation>>,
    pub(super) get_fetch_service: Arc<crate::services::GetFetchService>,
}

impl SendGetRequestsCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            get_operation_status_service: Arc::clone(context.get_operation_status_service()),
            get_fetch_service: Arc::clone(context.get_fetch_service()),
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
        let operation_id = data.operation_id;

        let fetch_request = GetFetchRequest {
            operation_id,
            ual: data.ual.clone(),
            include_metadata: data.include_metadata,
            paranet_ual: data.paranet_ual.clone(),
            visibility: data.visibility,
        };

        match self.get_fetch_service.fetch(&fetch_request).await {
            Ok(result) => {
                let get_result = GetOperationResult::new(result.assertion, result.metadata);
                if let Err(e) = self
                    .get_operation_status_service
                    .complete_with_result(operation_id, get_result)
                    .await
                {
                    tracing::error!(
                        operation_id = %operation_id,
                        error = %e,
                        "Failed to complete get operation"
                    );
                    self.get_operation_status_service
                        .mark_failed(operation_id, e.to_string())
                        .await;
                }
            }
            Err(error_message) => {
                tracing::warn!(
                    operation_id = %operation_id,
                    error = %error_message,
                    "Get operation failed"
                );
                self.get_operation_status_service
                    .mark_failed(operation_id, error_message)
                    .await;
            }
        }

        CommandOutcome::Completed
    }
}
