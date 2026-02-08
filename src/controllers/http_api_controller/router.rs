use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use axum::{
    Router,
    routing::{get, post},
};
use serde::Deserialize;
use tokio::{
    net::TcpListener,
    sync::{Mutex, oneshot},
};
use tower_http::{cors::CorsLayer, limit::RequestBodyLimitLayer, trace::TraceLayer};

use super::{
    middleware::{AuthConfig, RateLimiterConfig},
    v1::{
        info::InfoHttpApiController, operation_result::OperationResultHttpApiController,
        publish_finality::PublishFinalityStatusHttpApiController,
        publish_store::PublishStoreHttpApiController,
    },
};
use crate::{context::Context, controllers::http_api_controller::v1::get::GetHttpApiController};

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpApiConfig {
    /// Whether the HTTP API server is enabled.
    pub enabled: bool,
    pub port: u16,
    pub rate_limiter: RateLimiterConfig,
    pub auth: AuthConfig,
}

pub(crate) struct HttpApiRouter {
    config: HttpApiConfig,
    router: Arc<Mutex<Router>>,
}

/// Maximum request body size in bytes (10 MB)
const MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

impl HttpApiRouter {
    pub(crate) fn new(config: &HttpApiConfig, context: &Arc<Context>) -> Self {
        // Build the base router with routes and state
        let mut router = Router::new()
            .route("/v1/info", get(InfoHttpApiController::handle_request))
            .route(
                "/v1/publish",
                post(PublishStoreHttpApiController::handle_request),
            )
            .route(
                "/v1/publish/{operation_id}",
                get(OperationResultHttpApiController::handle_publish_result),
            )
            .route(
                "/v1/finality",
                get(PublishFinalityStatusHttpApiController::handle_request),
            )
            .route("/v1/get", post(GetHttpApiController::handle_request))
            .route(
                "/v1/get/{operation_id}",
                get(OperationResultHttpApiController::handle_get_result),
            )
            .with_state(Arc::clone(context));

        // Layer order (bottom-to-top, last added runs first):
        // 1. Auth middleware (innermost - runs first for security)
        // 2. Rate limiter
        // 3. Body size limit
        // 4. Request tracing
        // 5. CORS (outermost)

        // 1. Apply auth middleware if enabled
        if let Some(layer) = config.auth.build_layer() {
            router = router.layer(layer);
            tracing::info!(
                "Auth middleware enabled: IP whitelist = {:?}",
                config.auth.ip_whitelist
            );
        } else {
            tracing::info!("Auth middleware disabled");
        }

        // 2. Apply rate limiter middleware if enabled
        if let Some(layer) = config.rate_limiter.build_layer() {
            router = router.layer(layer);
            tracing::info!(
                "Rate limiter enabled: {} requests per {} seconds (burst: {})",
                config.rate_limiter.max_requests,
                config.rate_limiter.time_window_seconds,
                config.rate_limiter.effective_burst_size()
            );
        } else {
            tracing::info!("Rate limiter disabled");
        }

        // 3. Apply body size limit
        router = router.layer(RequestBodyLimitLayer::new(MAX_BODY_SIZE));
        tracing::info!("Request body limit: {} MB", MAX_BODY_SIZE / (1024 * 1024));

        // 4. Apply request tracing
        router = router.layer(TraceLayer::new_for_http());
        tracing::info!("Request tracing enabled");

        // 5. Apply CORS layer (outermost)
        router = router.layer(CorsLayer::permissive());

        HttpApiRouter {
            config: config.to_owned(),
            router: Arc::new(Mutex::new(router)),
        }
    }

    pub(crate) async fn listen_and_handle_http_requests(&self, shutdown_rx: oneshot::Receiver<()>) {
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, self.config.port));

        let cloned_router_for_serve = self.router.lock().await.clone();

        let listener = TcpListener::bind(addr)
            .await
            .expect("Failed to bind HTTP listener");

        // Use into_make_service_with_connect_info to make client IP available
        // to the auth middleware via ConnectInfo<SocketAddr> in request extensions
        axum::serve(
            listener,
            cloned_router_for_serve.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
            tracing::info!("HTTP server shutting down gracefully");
        })
        .await
        .expect("Server failed");
    }
}
