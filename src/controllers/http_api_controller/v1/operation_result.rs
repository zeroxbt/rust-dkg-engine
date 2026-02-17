use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use dkg_repository::OperationStatus;
use uuid::Uuid;

use crate::{
    context::Context,
    controllers::http_api_controller::v1::dto::{
        get::GetOperationResultResponse,
        operation_result::{OperationResultErrorResponse, OperationResultResponse, SignatureData},
    },
    operations::{GetOperation, OperationKind, PublishStoreOperation, PublishStoreOperationResult},
};

pub(crate) struct OperationResultHttpApiController;

impl OperationResultHttpApiController {
    pub(crate) async fn handle_publish_result(
        State(context): State<Arc<Context>>,
        Path(operation_id): Path<String>,
    ) -> impl IntoResponse {
        // Publish polling covers the store phase only (signatures), not finality.
        // Validate operation ID format
        let Ok(operation_uuid) = Uuid::parse_str(&operation_id) else {
            return (
                StatusCode::BAD_REQUEST,
                Json(OperationResultErrorResponse::new(
                    400,
                    format!("Operation id: {} is in wrong format", operation_id),
                )),
            )
                .into_response();
        };

        // Get operation record
        let operation_record = match context
            .repository_manager()
            .operation_repository()
            .get_by_id_and_name(operation_uuid, PublishStoreOperation::NAME)
            .await
        {
            Ok(Some(record)) => record,
            Ok(None) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(OperationResultErrorResponse::new(
                        400,
                        format!(
                            "Handler with id: {} does not exist or is not a publish operation.",
                            operation_id
                        ),
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

        let status = match operation_record.operation_status() {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Invalid operation status in database"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(OperationResultErrorResponse::new(
                        500,
                        "Invalid operation status".to_string(),
                    )),
                )
                    .into_response();
            }
        };

        match status {
            OperationStatus::Failed => {
                let response = OperationResultResponse::failed(operation_record.error_message);
                (StatusCode::OK, Json(response)).into_response()
            }
            OperationStatus::Completed => {
                // Get signatures from redb
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
                            "Failed to get publish result"
                        );
                        let response = OperationResultResponse::failed(Some(
                            "Failed to retrieve result".into(),
                        ));
                        (StatusCode::OK, Json(response)).into_response()
                    }
                }
            }
            OperationStatus::InProgress => {
                let response = OperationResultResponse::in_progress();
                (StatusCode::OK, Json(response)).into_response()
            }
        }
    }

    /// Get signatures from redb storage
    async fn get_signatures(
        context: &Arc<Context>,
        operation_id: Uuid,
    ) -> Result<(Option<SignatureData>, Vec<SignatureData>), String> {
        // Get result from redb via publish operation service
        let result: Option<PublishStoreOperationResult> = context
            .publish_store_operation_status_service()
            .get_result(operation_id)
            .await
            .map_err(|e| e.to_string())?;

        match result {
            Some(publish_result) => {
                // Convert from operations::PublishStoreSignatureData to dto::SignatureData
                let publisher_sig = publish_result.publisher_signature.map(|sig| SignatureData {
                    identity_id: sig.identity_id,
                    v: sig.v,
                    r: sig.r,
                    s: sig.s,
                    vs: sig.vs,
                });

                let network_sigs = publish_result
                    .network_signatures
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
            None => Ok((None, Vec::new())),
        }
    }

    pub(crate) async fn handle_get_result(
        State(context): State<Arc<Context>>,
        Path(operation_id): Path<String>,
    ) -> impl IntoResponse {
        // Validate operation ID format
        let Ok(operation_uuid) = Uuid::parse_str(&operation_id) else {
            return (
                StatusCode::BAD_REQUEST,
                Json(OperationResultErrorResponse::new(
                    400,
                    format!("Operation id: {} is in wrong format", operation_id),
                )),
            )
                .into_response();
        };

        // Get operation record
        let operation_record = match context
            .repository_manager()
            .operation_repository()
            .get_by_id_and_name(operation_uuid, GetOperation::NAME)
            .await
        {
            Ok(Some(record)) => record,
            Ok(None) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(OperationResultErrorResponse::new(
                        400,
                        format!(
                            "Handler with id: {} does not exist or is not a get operation.",
                            operation_id
                        ),
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

        let status = match operation_record.operation_status() {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    operation_id = %operation_id,
                    error = %e,
                    "Invalid operation status in database"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(OperationResultErrorResponse::new(
                        500,
                        "Invalid operation status".to_string(),
                    )),
                )
                    .into_response();
            }
        };

        match status {
            OperationStatus::Failed => {
                let response = GetOperationResultResponse::failed(operation_record.error_message);
                (StatusCode::OK, Json(response)).into_response()
            }
            OperationStatus::Completed => {
                // Get result from redb store via new operation service
                match context
                    .get_operation_status_service()
                    .get_result(operation_uuid)
                    .await
                {
                    Ok(Some(result)) => {
                        let response = GetOperationResultResponse::completed(
                            result.assertion,
                            result.metadata,
                        );
                        (StatusCode::OK, Json(response)).into_response()
                    }
                    Ok(None) => {
                        tracing::error!(
                            operation_id = %operation_id,
                            "Operation marked as completed but no cached result found"
                        );
                        let response = GetOperationResultResponse::failed(Some(
                            "Operation result not found".to_string(),
                        ));
                        (StatusCode::OK, Json(response)).into_response()
                    }
                    Err(e) => {
                        tracing::error!(
                            operation_id = %operation_id,
                            error = %e,
                            "Failed to retrieve operation result"
                        );
                        let response = GetOperationResultResponse::failed(Some(
                            "Failed to retrieve operation result".to_string(),
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
