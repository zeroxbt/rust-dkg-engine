use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use hyper::StatusCode;
use validator::Validate;

use crate::{
    commands::publish_replication_command::PublishReplicationCommandData,
    context::Context,
    types::{
        dto::publish::{PublishRequest, PublishResponse},
        models::OperationId,
        traits::{command::CommandData, service::OperationLifecycle},
    },
};

pub struct PublishController;

impl PublishController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<PublishRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                // Generate operation ID
                let operation_id = match context.publish_service().create_operation_record().await {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::error!("Failed to create operation record: {}", e);
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Failed to create operation: {}", e),
                        )
                            .into_response();
                    }
                };

                // Spawn async task to execute the operation
                tokio::spawn(async move {
                    Self::execute_publish_operation(Arc::clone(&context), req, operation_id).await
                });

                // Return operation ID immediately
                Json(PublishResponse::new(operation_id)).into_response()
            }
            Err(e) => {
                let error_messages: Vec<String> = e
                    .field_errors()
                    .iter()
                    .map(|(field, errors)| {
                        let messages: Vec<String> = errors
                            .iter()
                            .filter_map(|err| err.message.as_ref().map(|m| m.to_string()))
                            .collect();
                        format!("{}: {}", field, messages.join(", "))
                    })
                    .collect();

                (
                    StatusCode::BAD_REQUEST,
                    format!("Validation error: {}", error_messages.join("; ")),
                )
                    .into_response()
            }
        }
    }

    async fn execute_publish_operation(
        context: Arc<Context>,
        request: PublishRequest,
        operation_id: OperationId,
    ) {
        // Destructure request early to take ownership and avoid clones later
        let PublishRequest {
            blockchain,
            dataset_root,
            dataset,
            minimum_number_of_node_replications,
            ..
        } = request;

        tracing::info!(
            "Starting publish operation - operation_id: {}, dataset_root: {}, blockchain: {}",
            operation_id,
            dataset_root,
            blockchain
        );

        if let Err(e) = context
            .pending_storage_service()
            .store_dataset(operation_id, &dataset_root, &dataset)
            .await
        {
            tracing::error!(
                "Failed to store dataset for operation {}: {}",
                operation_id,
                e
            );
            Self::handle_operation_failure(&context, operation_id, Box::new(e), "store dataset")
                .await;
            return;
        }

        tracing::debug!(
            "Dataset stored successfully for operation {} (dataset_root: {})",
            operation_id,
            dataset_root
        );

        let command = PublishReplicationCommandData::new(
            operation_id,
            blockchain.clone(),
            dataset_root.clone(),
            minimum_number_of_node_replications,
        )
        .into_command();

        if let Err(e) = context.schedule_command_tx().send(command).await {
            tracing::error!(
                "Failed to schedule publish command for operation {}: {}",
                operation_id,
                e
            );
            Self::handle_operation_failure(&context, operation_id, Box::new(e), "schedule command")
                .await;
            return;
        }

        tracing::info!(
            "Publish command scheduled successfully for operation {} (dataset_root: {}, blockchain: {})",
            operation_id,
            dataset_root,
            blockchain
        );
    }

    async fn handle_operation_failure(
        context: &Arc<Context>,
        operation_id: OperationId,
        error: Box<dyn std::error::Error + Send + Sync>,
        stage: &str,
    ) {
        if let Err(mark_error) = context
            .publish_service()
            .mark_failed(operation_id, error)
            .await
        {
            tracing::error!(
                "Unable to mark operation {} as failed (stage: {}): {}",
                operation_id,
                stage,
                mark_error
            );
        }
    }
}
