use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    response::IntoResponse,
};
use hyper::StatusCode;
use validator::Validate;

use crate::{
    context::Context,
    controllers::http_api_controller::v1::dto::finality::{
        FinalityRequest, FinalityStatusErrorResponse, FinalityStatusResponse,
    },
};

pub(crate) struct FinalityStatusHttpApiController;

impl FinalityStatusHttpApiController {
    pub(crate) async fn   handle_request(
        State(context): State<Arc<Context>>,
        Query(req): Query<FinalityRequest>,
    ) -> impl IntoResponse {
        if let Err(e) = req.validate() {
            let error_messages: Vec<String> = e
                .field_errors()
                .iter()
                .map(|(field, errors)| {
                    let messages: Vec<String> = errors
                        .iter()
                        .filter_map(|err| err.message.as_ref().map(|m| m.to_string()))
                        .collect();
                    if messages.is_empty() {
                        format!("{}: invalid value", field)
                    } else {
                        format!("{}: {}", field, messages.join(", "))
                    }
                })
                .collect();

            return (
                StatusCode::BAD_REQUEST,
                Json(FinalityStatusErrorResponse::new(error_messages.join("; "))),
            )
                .into_response();
        }

        match context
            .repository_manager()
            .finality_status_repository()
            .get_finality_acks_count(&req.ual)
            .await
        {
            Ok(count) => (StatusCode::OK, Json(FinalityStatusResponse::new(count))).into_response(),
            Err(e) => {
                tracing::error!(ual = %req.ual, error = %e, "Failed to get finality acks count");
                (
                    StatusCode::BAD_REQUEST,
                    Json(FinalityStatusErrorResponse::new(
                        "Asset with provided UAL was not published to this node.",
                    )),
                )
                    .into_response()
            }
        }
    }
}
