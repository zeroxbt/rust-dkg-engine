use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use hyper::StatusCode;
use uuid::Uuid;
use validator::Validate;

use crate::{
    commands::{
        command_executor::CommandExecutionRequest, command_registry::Command,
        operations::publish::protocols::store::send_store_requests_command::SendStoreRequestsCommandData,
    },
    context::Context,
    controllers::http_api_controller::v1::dto::publish::{PublishRequest, PublishResponse},
};

pub struct PublishHttpApiController;

impl PublishHttpApiController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<PublishRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                let operation_id = Uuid::new_v4();

                if let Err(e) = context
                    .publish_operation_service()
                    .create_operation(operation_id)
                    .await
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
                let command = Command::SendStoreRequests(SendStoreRequestsCommandData::new(
                    operation_id,
                    req.blockchain,
                    req.dataset_root,
                    req.minimum_number_of_node_replications,
                    req.dataset,
                ));

                let request = CommandExecutionRequest::new(command);

                if let Err(e) = context.schedule_command_tx().send(request).await {
                    tracing::error!(operation_id = %operation_id, error = %e, "Failed to schedule SendPublishRequestsCommand");
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
