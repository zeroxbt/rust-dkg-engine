use std::sync::Arc;

use crate::{
    application::{GetAssertionInput, GetAssertionUseCase, OperationTracking},
    operations::{GetOperation, GetOperationResult},
};

/// Bundles assertion retrieval with operation tracking so that command handlers
/// remain pure adapters.  `execute` is fire-and-forget: it handles errors
/// internally by marking the operation as failed.
pub(crate) struct GetOperationWorkflow {
    get_assertion_use_case: Arc<GetAssertionUseCase>,
    get_operation_tracking: Arc<OperationTracking<GetOperation>>,
}

impl GetOperationWorkflow {
    pub(crate) fn new(
        get_assertion_use_case: Arc<GetAssertionUseCase>,
        get_operation_tracking: Arc<OperationTracking<GetOperation>>,
    ) -> Self {
        Self {
            get_assertion_use_case,
            get_operation_tracking,
        }
    }

    /// Fire-and-forget: handles errors internally by marking the operation as failed.
    pub(crate) async fn execute(&self, input: &GetAssertionInput) {
        let operation_id = input.operation_id;

        match self.get_assertion_use_case.fetch(input).await {
            Ok(result) => {
                let get_result = GetOperationResult::new(result.assertion, result.metadata);
                if let Err(e) = self
                    .get_operation_tracking
                    .complete_with_result(operation_id, get_result)
                    .await
                {
                    tracing::error!(
                        operation_id = %operation_id,
                        error = %e,
                        "Failed to complete get operation"
                    );
                    self.get_operation_tracking
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
                self.get_operation_tracking
                    .mark_failed(operation_id, error_message)
                    .await;
            }
        }
    }
}
