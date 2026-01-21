use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use hyper::StatusCode;
use uuid::Uuid;
use validator::Validate;

use crate::{
    commands::{
        command_executor::CommandExecutionRequest, command_registry::Command,
        operations::get::protocols::get::send_get_requests_command::SendGetRequestsCommandData,
    },
    context::Context,
    controllers::http_api_controller::v1::dto::get::{GetRequest, GetResponse},
};

pub struct GetHttpApiController;

impl GetHttpApiController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<GetRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                let operation_id = Uuid::new_v4();

                if let Err(e) = context
                    .get_operation_service()
                    .create_operation(operation_id)
                    .await
                {
                    tracing::error!(operation_id = %operation_id, error = %e, "Failed to create get operation record");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Failed to create operation: {}", e),
                    )
                        .into_response();
                }

                tracing::info!(operation_id = %operation_id, ual = %req.id, "Get request received");

                let command = Command::SendGetRequests(SendGetRequestsCommandData::new(
                    operation_id,
                    req.id,
                    req.include_metadata,
                    req.paranet_ual,
                    req.content_type,
                ));

                context
                    .command_scheduler()
                    .schedule(CommandExecutionRequest::new(command))
                    .await;

                Json(GetResponse::new(operation_id)).into_response()
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
