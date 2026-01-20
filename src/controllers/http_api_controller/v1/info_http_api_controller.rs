use axum::Json;

use crate::types::dto::info::InfoResponse;

pub struct InfoHttpApiController;

impl InfoHttpApiController {
    pub async fn handle_request() -> Json<InfoResponse> {
        Json(InfoResponse::new("6.0.12"))
    }
}
