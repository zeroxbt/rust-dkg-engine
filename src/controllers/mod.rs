pub(crate) mod http_api_controller;
pub(crate) mod rpc_controller;

pub(crate) use http_api_controller::router::{HttpApiConfig, HttpApiRouter};
pub(crate) use rpc_controller::rpc_router::RpcRouter;
use serde::{Deserialize, Serialize};

use crate::controllers::{
    http_api_controller::HttpApiDeps,
    rpc_controller::{RpcConfig, RpcRouterDeps},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ControllersConfig {
    pub http_api: HttpApiConfig,
    pub rpc: RpcConfig,
}

/// Container for all initialized controllers.
pub(crate) struct Controllers {
    pub http_router: Option<HttpApiRouter>,
    pub rpc_router: RpcRouter,
}

/// Initialize all controllers.
pub(crate) fn initialize(
    config: &ControllersConfig,
    rpc_router_deps: RpcRouterDeps,
    http_api_deps: HttpApiDeps,
) -> Controllers {
    let http_router = if config.http_api.enabled {
        tracing::info!("HTTP API enabled on port {}", config.http_api.port);
        Some(HttpApiRouter::new(&config.http_api, http_api_deps))
    } else {
        tracing::info!("HTTP API disabled");
        None
    };

    let rpc_router = RpcRouter::new(rpc_router_deps, &config.rpc);

    Controllers {
        http_router,
        rpc_router,
    }
}
