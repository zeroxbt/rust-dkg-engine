use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    response::IntoResponse,
};
use hyper::StatusCode;

use crate::{
    context::Context,
    controllers::http_api_controller::v1::dto::finality::{
        FinalityRequest, FinalityStatusErrorResponse, FinalityStatusResponse,
    },
};

pub struct FinalityStatusHttpApiController;

impl FinalityStatusHttpApiController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Query(req): Query<FinalityRequest>,
    ) -> impl IntoResponse {
        if req.ual.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(FinalityStatusErrorResponse::new(
                    "Asset with provided UAL was not published to this node.",
                )),
            )
                .into_response();
        }

        match context
            .repository_manager()
            .finality_status_repository()
            .get_finality_acks_count(&req.ual)
            .await
        {
            Ok(count) => {
                if count == 0 {
                    // Check if the UAL exists at all - if count is 0, it might not have been
                    // published
                    (StatusCode::OK, Json(FinalityStatusResponse::new(count))).into_response()
                } else {
                    (StatusCode::OK, Json(FinalityStatusResponse::new(count))).into_response()
                }
            }
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
