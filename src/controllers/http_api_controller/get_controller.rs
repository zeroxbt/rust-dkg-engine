use crate::context::Context;
use crate::types::dto::get::{GetRequest, GetResponse};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use std::sync::Arc;
use uuid::Uuid;
use validator::Validate;

pub struct GetController;

impl GetController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<GetRequest>,
    ) -> impl IntoResponse {
        match req.validate() {
            Ok(_) => {
                let operation_id = Uuid::new_v4();

                tokio::spawn(async move {
                    Self::execute_get_operation(Arc::clone(&context), req, operation_id).await
                });

                Json(GetResponse::new(operation_id)).into_response()
            }
            Err(e) => (
                StatusCode::BAD_REQUEST,
                format!("Validation error: {:?}", e),
            )
                .into_response(),
        }
    }

    async fn execute_get_operation(context: Arc<Context>, request: GetRequest, operation_id: Uuid) {
    }
}
