use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use hyper::StatusCode;
use uuid::Uuid;
use validator::Validate;

use crate::{
    commands::protocols::publish::send_publish_requests_command::SendPublishRequestsCommandData,
    context::Context,
    types::{
        dto::publish::{PublishRequest, PublishResponse},
        traits::command::CommandData,
    },
};

const MIN_ACK_RESPONSES: u8 = 8;

pub struct PublishHttpApiController;

impl PublishHttpApiController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<PublishRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                // Generate operation ID and create DB record immediately
                let operation_id = Uuid::new_v4();

                // Create operation record in DB before spawning
                if let Err(e) = context
                    .publish_operation_manager()
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

                // Schedule the command with dataset passed inline
                let command = SendPublishRequestsCommandData::new(
                    operation_id,
                    req.blockchain,
                    req.dataset_root,
                    req.minimum_number_of_node_replications
                        .unwrap_or(MIN_ACK_RESPONSES),
                    req.dataset,
                )
                .into_command();

                if let Err(e) = context.schedule_command_tx().send(command).await {
                    tracing::error!(operation_id = %operation_id, error = %e, "Failed to schedule SendPublishRequestsCommand");
                }

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
}
