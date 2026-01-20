use axum::Json;

use crate::controllers::http_api_controller::v1::dto::info::InfoResponse;

pub struct InfoHttpApiController;

impl InfoHttpApiController {
    pub async fn handle_request() -> Json<InfoResponse> {
        Json(InfoResponse::new("6.0.12"))
    }
}
