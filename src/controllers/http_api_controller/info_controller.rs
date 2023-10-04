use axum::Json;
use serde::Serialize;

pub struct InfoController;

#[derive(Serialize)]
pub struct InfoResponse {
    version: &'static str,
}

impl InfoController {
    pub async fn handle_request() -> Json<InfoResponse> {
        Json(InfoResponse { version: "6.0.12" })
    }
}
