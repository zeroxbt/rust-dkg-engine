use std::{str::FromStr, sync::Arc};

use axum::{
    Json,
    extract::{Path, State},
    response::IntoResponse,
};
use hyper::StatusCode;
use repository::OperationStatus;
use uuid::Uuid;

use crate::{
    context::Context,
    controllers::http_api_controller::v1::dto::{
        get::GetOperationResultResponse,
        operation_result::{OperationResultErrorResponse, OperationResultResponse, SignatureData},
    },
    services::GetOperationResult,
};

pub struct OperationResultHttpApiController;

impl OperationResultHttpApiController {
    pub async fn handle_publish_result(
        State(context): State<Arc<Context>>,
        Path(operation_id): Path<String>,
    ) -> impl IntoResponse {
        // Validate operation ID format
        let operation_uuid = match Uuid::parse_str(&operation_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(OperationResultErrorResponse::new(
                        400,
                        format!("Operation id: {} is in wrong format", operation_id),
                    )),
                )
                    .into_response();
            }
        };

        // Get operation record
        let operation_record = match context
            .repository_manager()
            .operation_repository()
            .get(operation_uuid)
            .await
        {
            Ok(Some(record)) => record,
            Ok(None) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(OperationResultErrorResponse::new(
                        400,
                        format!("Handler with id: {} does not exist.", operation_id),
                    )),
                )
                    .into_response();
            }
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to get operation record"
                );
                return (
                    StatusCode::BAD_REQUEST,
                    Json(OperationResultErrorResponse::new(
                        400,
                        format!("Unexpected error at getting results: {}", e),
                    )),
                )
                    .into_response();
            }
        };

        let status = OperationStatus::from_str(&operation_record.status).unwrap();

        // Check if min acks reached
        let min_acks_reached = operation_record
            .min_ack_responses
            .map(|min| operation_record.completed_count >= min)
            .unwrap_or(false);

        match status {
            OperationStatus::Failed => {
                let response = OperationResultResponse::failed(operation_record.error_message);
                (StatusCode::OK, Json(response)).into_response()
            }
            OperationStatus::Completed | OperationStatus::InProgress if min_acks_reached => {
                // Get signatures when min acks reached
                match Self::get_signatures(&context, operation_uuid).await {
                    Ok((publisher_sig, network_sigs)) => {
                        let response = OperationResultResponse::completed_with_signatures(
                            publisher_sig,
                            network_sigs,
                        );
                        (StatusCode::OK, Json(response)).into_response()
                    }
                    Err(e) => {
                        tracing::error!(
                            operation_id = %operation_id,
                            error = %e,
                            "Failed to get signatures"
                        );
                        let response = OperationResultResponse::in_progress_with_data(true);
                        (StatusCode::OK, Json(response)).into_response()
                    }
                }
            }
            OperationStatus::InProgress => {
                let response = OperationResultResponse::in_progress_with_data(false);
                (StatusCode::OK, Json(response)).into_response()
            }
            _ => {
                let response = OperationResultResponse::pending();
                (StatusCode::OK, Json(response)).into_response()
            }
        }
    }

    async fn get_signatures(
        context: &Arc<Context>,
        operation_id: Uuid,
    ) -> Result<(Option<SignatureData>, Vec<SignatureData>), String> {
        let repo = context.repository_manager().signature_repository();

        // Get publisher signature
        let publisher_sig = repo
            .get_publisher_signature(operation_id)
            .await
            .map_err(|e| e.to_string())?
            .map(|sig| SignatureData {
                identity_id: sig.identity_id,
                v: sig.v,
                r: sig.r,
                s: sig.s,
                vs: sig.vs,
            });

        // Get network signatures
        let network_sigs = repo
            .get_network_signatures(operation_id)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(|sig| SignatureData {
                identity_id: sig.identity_id,
                v: sig.v,
                r: sig.r,
                s: sig.s,
                vs: sig.vs,
            })
            .collect();

        Ok((publisher_sig, network_sigs))
    }

    pub async fn handle_get_result(
        State(context): State<Arc<Context>>,
        Path(operation_id): Path<String>,
    ) -> impl IntoResponse {
        // Validate operation ID format
        let operation_uuid = match Uuid::parse_str(&operation_id) {
            Ok(uuid) => uuid,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(OperationResultErrorResponse::new(
                        400,
                        format!("Operation id: {} is in wrong format", operation_id),
                    )),
                )
                    .into_response();
            }
        };

        // Get operation record
        let operation_record = match context
            .repository_manager()
            .operation_repository()
            .get(operation_uuid)
            .await
        {
            Ok(Some(record)) => record,
            Ok(None) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(OperationResultErrorResponse::new(
                        400,
                        format!("Handler with id: {} does not exist.", operation_id),
                    )),
                )
                    .into_response();
            }
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Failed to get operation record"
                );
                return (
                    StatusCode::BAD_REQUEST,
                    Json(OperationResultErrorResponse::new(
                        400,
                        format!("Unexpected error at getting results: {}", e),
                    )),
                )
                    .into_response();
            }
        };

        let status = OperationStatus::from_str(&operation_record.status).unwrap();

        match status {
            OperationStatus::Failed => {
                let response = GetOperationResultResponse::failed(operation_record.error_message);
                (StatusCode::OK, Json(response)).into_response()
            }
            OperationStatus::Completed => {
                // Get cached result from file
                match context
                    .get_operation_manager()
                    .get_cached_result::<GetOperationResult>(operation_uuid)
                    .await
                {
                    Some(result) => {
                        let response = GetOperationResultResponse::completed(
                            result.assertion,
                            result.metadata,
                        );
                        (StatusCode::OK, Json(response)).into_response()
                    }
                    None => {
                        tracing::error!(
                            operation_id = %operation_id,
                            "Operation marked as completed but no cached result found"
                        );
                        let response = GetOperationResultResponse::failed(Some(
                            "Operation result not found".to_string(),
                        ));
                        (StatusCode::OK, Json(response)).into_response()
                    }
                }
            }
            OperationStatus::InProgress => {
                let response = GetOperationResultResponse::in_progress();
                (StatusCode::OK, Json(response)).into_response()
            }
        }
    }
}
