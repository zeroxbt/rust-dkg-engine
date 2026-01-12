use crate::types::dto::info::InfoResponse;
use axum::Json;

pub struct InfoController;

impl InfoController {
    pub async fn handle_request() -> Json<InfoResponse> {
        Json(InfoResponse::new("6.0.12"))
    }
}
