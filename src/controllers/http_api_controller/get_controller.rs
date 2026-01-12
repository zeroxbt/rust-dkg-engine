use std::sync::Arc;

use axum::{Json, extract::State, response::IntoResponse};
use hyper::StatusCode;
use uuid::Uuid;
use validator::Validate;

use crate::{
    context::Context,
    types::dto::get::{GetRequest, GetResponse},
};

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
