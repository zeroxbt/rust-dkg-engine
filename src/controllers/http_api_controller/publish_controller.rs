use crate::commands::command::CommandData;
use crate::commands::publish_replication_command::PublishReplicationCommandData;
use crate::context::Context;
use crate::services::operation_service::OperationLifecycle;
use crate::types::dto::publish::{PublishRequest, PublishResponse};
use crate::types::models::OperationId;
use axum::Json;
use axum::{extract::State, response::IntoResponse};
use hyper::StatusCode;
use std::sync::Arc;
use validator::Validate;

pub struct PublishController;

impl PublishController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<PublishRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                // Generate operation ID
                let operation_id = context
                    .publish_service()
                    .create_operation_record()
                    .await
                    .unwrap();

                // Spawn async task to execute the operation
                tokio::spawn(async move {
                    Self::execute_publish_operation(Arc::clone(&context), req, operation_id).await
                });

                // Return operation ID immediately
                Json(PublishResponse::new(operation_id)).into_response()
            }
            Err(e) => (
                StatusCode::BAD_REQUEST,
                format!("Validation error: {:?}", e),
            )
                .into_response(),
        }
    }

    async fn execute_publish_operation(
        context: Arc<Context>,
        request: PublishRequest,
        operation_id: OperationId,
    ) {
        tracing::info!(
            "Received asset with dataset root: {}, blockchain: {}",
            request.dataset_root,
            request.blockchain
        );

        let res = context
            .pending_storage_service()
            .store_dataset(operation_id, &request.dataset_root, &request.dataset)
            .await;

        let command = PublishReplicationCommandData::new(
            operation_id,
            request.blockchain.clone(),
            request.dataset_root.clone(),
            request.minimum_number_of_node_replications,
        )
        .into_command();

        let schedule_result = context.schedule_command_tx().send(command).await;

        if let Err(e) = schedule_result {
            let mark_result = context
                .publish_service()
                .mark_failed(operation_id, Box::new(e))
                .await;
            if let Err(e) = mark_result {
                tracing::error!(
                    "Unable to mark operation with id: {operation_id} as failed. Error: {e}"
                );
            }
        }
    }
}
