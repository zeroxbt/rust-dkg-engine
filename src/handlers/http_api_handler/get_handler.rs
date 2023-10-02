use crate::context::Context;
use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

pub struct GetHandler;

#[derive(Deserialize, Debug)]
pub struct GetRequest {}

#[derive(Serialize, Debug, Clone)]
pub struct GetResponse {
    operation_id: Uuid,
}

impl GetHandler {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Json(req): Json<GetRequest>,
    ) -> Json<GetResponse> {
        let operation_id = Uuid::new_v4();

        tokio::spawn(async move { Self::execute_get_operation(context, req, operation_id).await });

        Json(GetResponse { operation_id })
    }

    async fn execute_get_operation(context: Arc<Context>, request: GetRequest, operation_id: Uuid) {
    }
}
