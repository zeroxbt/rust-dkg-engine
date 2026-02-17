pub(crate) mod http_api_controller;
pub(crate) mod rpc_controller;

pub(crate) use http_api_controller::router::{HttpApiConfig, HttpApiRouter};
pub(crate) use rpc_controller::rpc_router::RpcRouter;

use crate::controllers::{
    http_api_controller::HttpApiDeps,
    rpc_controller::{RpcConfig, RpcRouterDeps},
};

/// Container for all initialized controllers.
pub(crate) struct Controllers {
    pub http_router: Option<HttpApiRouter>,
    pub rpc_router: RpcRouter,
}

/// Initialize all controllers.
pub(crate) fn initialize(
    http_api_config: &HttpApiConfig,
    rpc_config: &RpcConfig,
    rpc_router_deps: RpcRouterDeps,
    http_api_deps: HttpApiDeps,
) -> Controllers {
    let http_router = if http_api_config.enabled {
        tracing::info!("HTTP API enabled on port {}", http_api_config.port);
        Some(HttpApiRouter::new(http_api_config, http_api_deps))
    } else {
        tracing::info!("HTTP API disabled");
        None
    };

    let rpc_router = RpcRouter::new(rpc_router_deps, rpc_config);

    Controllers {
        http_router,
        rpc_router,
    }
}
