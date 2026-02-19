use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use uuid::Uuid;
use validator::Validate;

use crate::{
    commands::{
        executor::CommandExecutionRequest,
        operations::publish::store::send_publish_store_requests::SendPublishStoreRequestsCommandData,
        registry::Command,
    },
    controllers::http_api_controller::{
        PublishStoreHttpApiControllerDeps,
        v1::dto::publish::{PublishRequest, PublishResponse},
    },
};

pub(crate) struct PublishStoreHttpApiController;

impl PublishStoreHttpApiController {
    pub(crate) async fn handle_request(
        State(context): State<PublishStoreHttpApiControllerDeps>,
        Json(req): Json<PublishRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                let operation_id = Uuid::new_v4();

                // Create operation record for the store phase (signatures).
                // Finality is handled separately; clients poll the result endpoint.
                if let Err(e) = context
                    .publish_store_operation_tracking
                    .create_operation(operation_id)
                    .await
                    .map(|_| ())
                {
                    tracing::error!(operation_id = %operation_id, error = %e, "Failed to create operation record");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to create operation: {}", e),
                    )
                        .into_response();
                }

                tracing::info!(operation_id = %operation_id, "Publish request received");

                // Pass user-provided value to command; command handler will determine effective
                // value
                let command =
                    Command::SendPublishStoreRequests(SendPublishStoreRequestsCommandData::new(
                        operation_id,
                        req.blockchain,
                        req.dataset_root,
                        req.minimum_number_of_node_replications,
                        req.dataset,
                    ));

                if !context
                    .command_scheduler
                    .try_schedule(CommandExecutionRequest::new(command))
                {
                    if let Err(e) = context
                        .publish_store_operation_tracking
                        .mark_failed(operation_id, "Command queue full".to_string())
                        .await
                    {
                        tracing::error!(
                            operation_id = %operation_id,
                            error = %e,
                            "Failed to mark publish operation as failed"
                        );
                    }

                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Node is busy, please retry.",
                    )
                        .into_response();
                }

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
}
