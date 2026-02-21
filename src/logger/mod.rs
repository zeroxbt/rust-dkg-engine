//! Logger initialization module.
//!
//! Configures tracing-based logging with support for:
//! - Pretty (human-readable) or JSON output formats
//! - Configurable log levels via config file or RUST_LOG env var
//! - Environment variable override (RUST_LOG takes precedence)
//! - Optional Prometheus metrics export

mod config;

use std::net::SocketAddr;

pub(crate) use config::{LogFormat, LoggerConfig, TelemetryConfig, TelemetryMetricsConfig};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize the global logger with the given configuration.
///
/// The `RUST_LOG` environment variable takes precedence over the config file setting.
/// If it is not set, the config value is used.
pub(crate) fn initialize(logger_config: &LoggerConfig, telemetry_config: &TelemetryConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&logger_config.level));

    initialize_logger(logger_config, filter);

    initialize_metrics(&telemetry_config.metrics);
}

fn initialize_logger(logger_config: &LoggerConfig, filter: EnvFilter) {
    match logger_config.format {
        LogFormat::Pretty => {
            let fmt_layer = fmt::layer()
                .with_target(true)
                .with_thread_ids(false)
                .with_file(false)
                .with_line_number(false);

            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .init();
        }
        LogFormat::Json => {
            let fmt_layer = fmt::layer().json();

            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .init();
        }
    }
}

fn initialize_metrics(metrics_config: &TelemetryMetricsConfig) {
    if !metrics_config.enabled {
        return;
    }

    let bind_address: SocketAddr = match metrics_config.bind_address.parse() {
        Ok(address) => address,
        Err(error) => {
            tracing::warn!(
                bind_address = %metrics_config.bind_address,
                error = %error,
                "Invalid metrics bind address; metrics exporter disabled"
            );
            return;
        }
    };

    match PrometheusBuilder::new()
        .with_http_listener(bind_address)
        .install()
    {
        Ok(_) => tracing::info!(
            bind_address = %bind_address,
            "Prometheus metrics exporter enabled"
        ),
        Err(error) => tracing::warn!(
            bind_address = %bind_address,
            error = %error,
            "Failed to initialize Prometheus metrics exporter"
        ),
    }
}
