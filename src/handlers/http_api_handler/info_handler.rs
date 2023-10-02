use axum::Json;
use serde::Serialize;

pub struct InfoHandler;

#[derive(Serialize)]
pub struct InfoResponse {
    version: &'static str,
}

impl InfoHandler {
    pub async fn handle_request() -> Json<InfoResponse> {
        Json(InfoResponse { version: "6.0.12" })
    }
}
