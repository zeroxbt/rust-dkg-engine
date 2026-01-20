use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use axum::{
    Router,
    http::Method,
    routing::{get, post},
};
use serde::Deserialize;
use tokio::{net::TcpListener, sync::Mutex};
use tower_http::cors::*;

use super::v1::{
    finality_http_api_controller::FinalityStatusHttpApiController,
    info_http_api_controller::InfoHttpApiController,
    operation_result_http_api_controller::OperationResultHttpApiController,
    publish_http_api_controller::PublishHttpApiController,
};
use crate::context::Context;

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
            .allow_methods(vec![Method::GET])
            .allow_credentials(false);

        let router = Router::new()
            .layer(cors_layer)
            .route("/v1/info", get(InfoHttpApiController::handle_request))
            .route(
                "/v1/publish",
                post(PublishHttpApiController::handle_request),
            )
            .route(
                "/v1/publish/{operation_id}",
                get(OperationResultHttpApiController::handle_publish_result),
            )
            .route(
                "/v1/finality",
                get(FinalityStatusHttpApiController::handle_request),
            )
            // .route("/get", post(GetController::handle_request))
            .with_state(Arc::clone(context));

        HttpApiRouter {
            config: config.to_owned(),
            router: Arc::new(Mutex::new(router)),
        }
    }

    pub async fn listen_and_handle_http_requests(&self) {
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, self.config.port));

        let cloned_router_for_serve = self.router.lock().await.clone();

        let listener = TcpListener::bind(addr)
            .await
            .expect("Failed to bind HTTP listener");

        axum::serve(listener, cloned_router_for_serve)
            .await
            .expect("Server failed");
    }
}
