pub(crate) mod http_api_controller;
pub(crate) mod rpc_controller;

use std::sync::Arc;

use crate::context::Context;

pub(crate) use http_api_controller::http_api_router::{HttpApiConfig, HttpApiRouter};
pub(crate) use rpc_controller::rpc_router::RpcRouter;

/// Container for all initialized controllers.
pub(crate) struct Controllers {
    pub http_router: Option<HttpApiRouter>,
    pub rpc_router: RpcRouter,
}

/// Initialize all controllers.
pub(crate) fn initialize(http_api_config: &HttpApiConfig, context: &Arc<Context>) -> Controllers {
    let http_router = if http_api_config.enabled {
        tracing::info!("HTTP API enabled on port {}", http_api_config.port);
        Some(HttpApiRouter::new(http_api_config, context))
    } else {
        tracing::info!("HTTP API disabled");
        None
    };

    let rpc_router = RpcRouter::new(Arc::clone(context));

    Controllers {
        http_router,
        rpc_router,
    }
}
