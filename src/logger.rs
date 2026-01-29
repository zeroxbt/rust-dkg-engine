//! Logger initialization module.
//!
//! Configures tracing-based logging with support for:
//! - Pretty (human-readable) or JSON output formats
//! - Configurable log levels via config file or RUST_LOG env var
//! - Environment variable override (RUST_LOG takes precedence)
//! - Optional OpenTelemetry export to Grafana Tempo via OTLP

use opentelemetry::{KeyValue, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::{LogFormat, LoggerConfig, TelemetryConfig};

/// Initialize the global logger with the given configuration.
///
/// The `RUST_LOG` environment variable takes precedence over the config file setting.
/// If neither is set, defaults to `rust_ot_node=info`.
pub(crate) fn initialize(logger_config: &LoggerConfig, telemetry_config: &TelemetryConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&logger_config.level));

    if telemetry_config.enabled {
        initialize_with_otel(logger_config, telemetry_config, filter);
    } else {
        initialize_without_otel(logger_config, filter);
    }
}

/// Initialize logger without OpenTelemetry.
fn initialize_without_otel(logger_config: &LoggerConfig, filter: EnvFilter) {
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

/// Create an OpenTelemetry tracer provider.
fn create_tracer_provider(
    telemetry_config: &TelemetryConfig,
) -> Result<opentelemetry_sdk::trace::TracerProvider, opentelemetry::trace::TraceError> {
    // Build the OTLP span exporter
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&telemetry_config.otlp_endpoint)
        .build()?;

    // Build the tracer provider with resource attributes
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(Resource::new(vec![
            KeyValue::new("service.name", telemetry_config.service_name.clone()),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        ]))
        .build();

    Ok(provider)
}

/// Initialize logger with OpenTelemetry tracing export.
fn initialize_with_otel(
    logger_config: &LoggerConfig,
    telemetry_config: &TelemetryConfig,
    filter: EnvFilter,
) {
    let provider = match create_tracer_provider(telemetry_config) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Failed to create OTLP exporter: {e}, falling back to non-OTEL logging");
            initialize_without_otel(logger_config, filter);
            return;
        }
    };

    // Set as global provider so shutdown works
    opentelemetry::global::set_tracer_provider(provider.clone());

    let tracer = provider.tracer("rust-ot-node");

    match logger_config.format {
        LogFormat::Pretty => {
            let fmt_layer = fmt::layer()
                .with_target(true)
                .with_thread_ids(false)
                .with_file(false)
                .with_line_number(false);

            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .with(otel_layer)
                .init();
        }
        LogFormat::Json => {
            let fmt_layer = fmt::layer().json();

            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .with(otel_layer)
                .init();
        }
    }

    tracing::info!(
        endpoint = %telemetry_config.otlp_endpoint,
        service = %telemetry_config.service_name,
        "OpenTelemetry tracing enabled"
    );
}

/// Shutdown OpenTelemetry gracefully, flushing any pending traces.
/// Call this before application exit.
pub(crate) fn shutdown_telemetry() {
    opentelemetry::global::shutdown_tracer_provider();
}
