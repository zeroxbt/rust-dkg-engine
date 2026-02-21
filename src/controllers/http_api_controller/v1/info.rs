use axum::Json;

use crate::controllers::http_api_controller::v1::dto::info::InfoResponse;

pub(crate) struct InfoHttpApiController;
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

impl InfoHttpApiController {
    pub(crate) async fn handle_request() -> Json<InfoResponse> {
        Json(InfoResponse::new(APP_VERSION))
    }
}
