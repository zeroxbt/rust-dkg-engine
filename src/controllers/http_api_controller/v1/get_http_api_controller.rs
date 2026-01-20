use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    response::IntoResponse,
};

use crate::{
    context::Context,
    controllers::http_api_controller::v1::schemas::get::{GetRequest, GetResponse},
};
pub struct GetHttpApiController;

impl GetHttpApiController {
    pub async fn handle_request(
        State(context): State<Arc<Context>>,
        Query(req): Query<GetRequest>,
    ) -> impl IntoResponse {
    }
}
