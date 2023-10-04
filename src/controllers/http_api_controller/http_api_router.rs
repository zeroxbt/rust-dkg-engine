use axum::routing::{get, post};
use axum::Router;
use hyper;
use hyper::Server;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower_http::cors::*;

use crate::context::Context;

use super::get_controller::GetController;
use super::info_controller::InfoController;
use super::publish_controller::PublishController;

#[derive(Clone, Debug, Deserialize)]
pub struct HttpApiConfig {
    pub port: u16,
}

pub struct HttpApiRouter {
    config: HttpApiConfig,
    router: Arc<Mutex<Router>>,
}

impl HttpApiRouter {
    pub fn new(config: &HttpApiConfig, context: &Arc<Context>) -> Self {
        let cors_layer = CorsLayer::new()
            .allow_methods(vec![hyper::Method::GET])
            .allow_credentials(false);

        let router = Router::new()
            .layer(cors_layer)
            .route("/info", get(InfoController::handle_request))
            .route("/publish", post(PublishController::handle_request))
            .route("/get", post(GetController::handle_request))
            .with_state(Arc::clone(context));

        HttpApiRouter {
            config: config.to_owned(),
            router: Arc::new(Mutex::new(router)),
        }
    }

    pub async fn handle_http_requests(&self) {
        let addr = ([127, 0, 0, 1], self.config.port).into();

        let cloned_router_for_serve = self.router.lock().await.clone(); // Clone it while locked

        Server::bind(&addr)
            .serve(cloned_router_for_serve.into_make_service())
            .await
            .expect("Server failed");
    }
}
