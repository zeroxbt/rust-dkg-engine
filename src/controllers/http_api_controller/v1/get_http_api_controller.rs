use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    response::IntoResponse,
};

use crate::{context::Context, types::dto::get::GetRequest};
pub struct GetHttpApiController;

impl GetHttpApiController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Query(req): Query<GetRequest>,
    ) -> impl IntoResponse {
    }
}
