use std::{
    future::Future,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
};

use axum::{
    Json,
    body::Body,
    extract::ConnectInfo,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tower::{Layer, Service};

/// Configuration for HTTP API authentication.
///
/// The configured IP whitelist controls access to the API.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthConfig {
    /// Whether authentication is enabled.
    pub enabled: bool,

    /// List of IP addresses allowed to access the API.
    pub ip_whitelist: Vec<String>,
}

impl AuthConfig {
    /// Build the authentication middleware layer.
    ///
    /// Returns `None` if authentication is disabled.
    pub(crate) fn build_layer(&self) -> Option<AuthLayer> {
        if !self.enabled {
            return None;
        }
        Some(AuthLayer {
            config: self.clone(),
        })
    }

    /// Check if the given IP address is in the whitelist.
    pub(crate) fn is_ip_allowed(&self, ip: &IpAddr) -> bool {
        let ip_str = ip.to_string();
        self.ip_whitelist.iter().any(|allowed| {
            // Direct match
            if allowed == &ip_str {
                return true;
            }

            // Handle IPv4-mapped IPv6 addresses (::ffff:127.0.0.1)
            if let IpAddr::V6(v6) = ip
                && let Some(v4) = v6.to_ipv4_mapped()
            {
                return allowed == &v4.to_string();
            }

            false
        })
    }
}

/// Authentication middleware layer.
#[derive(Clone)]
pub(crate) struct AuthLayer {
    config: AuthConfig,
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            config: self.config.clone(),
        }
    }
}

/// Authentication middleware service.
#[derive(Clone)]
pub(crate) struct AuthService<S> {
    inner: S,
    config: AuthConfig,
}

impl<S> Service<Request<Body>> for AuthService<S>
where
    S: Service<Request<Body>, Response = Response> + Clone + Send + 'static,
    S::Future: Send,
{
    type Response = Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let config = self.config.clone();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            // Extract client IP
            let client_ip = extract_client_ip(&req);

            match client_ip {
                Some(ip) => {
                    if config.is_ip_allowed(&ip) {
                        tracing::debug!(client_ip = %ip, "Request authorized");
                        inner.call(req).await
                    } else {
                        tracing::warn!(client_ip = %ip, "Request rejected: IP not in whitelist");
                        Ok(unauthorized_response())
                    }
                }
                None => {
                    tracing::warn!("Request rejected: Could not determine client IP");
                    Ok(unauthorized_response())
                }
            }
        })
    }
}

/// Extract client IP address from the request.
///
/// Checks X-Forwarded-For header first (for reverse proxy scenarios),
/// then falls back to the direct connection address from request extensions.
fn extract_client_ip(req: &Request<Body>) -> Option<IpAddr> {
    // Check X-Forwarded-For header first (leftmost IP is the client)
    if let Some(forwarded_for) = req.headers().get("x-forwarded-for")
        && let Ok(value) = forwarded_for.to_str()
        && let Some(first_ip) = value.split(',').next()
        && let Ok(ip) = first_ip.trim().parse::<IpAddr>()
    {
        return Some(ip);
    }

    // Fall back to direct connection address from request extensions
    // This is populated by axum when using into_make_service_with_connect_info
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip())
}

fn unauthorized_response() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({
            "error": "Unauthorized",
            "message": "Your IP address is not authorized to access this API."
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_default_whitelist_allows_localhost_ipv4() {
        let config = AuthConfig {
            enabled: true,
            ip_whitelist: vec!["127.0.0.1".to_string(), "::1".to_string()],
        };
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(config.is_ip_allowed(&ip));
    }

    #[test]
    fn test_default_whitelist_allows_localhost_ipv6() {
        let config = AuthConfig {
            enabled: true,
            ip_whitelist: vec!["127.0.0.1".to_string(), "::1".to_string()],
        };
        let ip: IpAddr = "::1".parse().unwrap();
        assert!(config.is_ip_allowed(&ip));
    }

    #[test]
    fn test_default_whitelist_rejects_external_ip() {
        let config = AuthConfig {
            enabled: true,
            ip_whitelist: vec!["127.0.0.1".to_string(), "::1".to_string()],
        };
        let ip: IpAddr = "192.168.1.100".parse().unwrap();
        assert!(!config.is_ip_allowed(&ip));
    }

    #[test]
    fn test_disabled_auth_allows_all() {
        let config = AuthConfig {
            enabled: false,
            ip_whitelist: vec![],
        };
        // When disabled, build_layer returns None
        assert!(config.build_layer().is_none());
    }

    #[test]
    fn test_enabled_auth_returns_layer() {
        let config = AuthConfig {
            enabled: true,
            ip_whitelist: vec!["127.0.0.1".to_string(), "::1".to_string()],
        };
        assert!(config.build_layer().is_some());
    }

    #[test]
    fn test_custom_whitelist() {
        let config = AuthConfig {
            enabled: true,
            ip_whitelist: vec!["10.0.0.1".to_string(), "192.168.1.100".to_string()],
        };
        assert!(config.is_ip_allowed(&"10.0.0.1".parse().unwrap()));
        assert!(config.is_ip_allowed(&"192.168.1.100".parse().unwrap()));
        assert!(!config.is_ip_allowed(&"127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn test_ipv4_mapped_ipv6() {
        let config = AuthConfig {
            enabled: true,
            ip_whitelist: vec!["127.0.0.1".to_string()],
        };
        // IPv4-mapped IPv6 address for 127.0.0.1
        let ip: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
        assert!(config.is_ip_allowed(&ip));
    }
}
