use serde::{Deserialize, Serialize};

/// Logger configuration for tracing output.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct LoggerConfig {
    /// Log level filter (e.g., "info", "debug", "trace", or module-specific like
    /// "rust_dkg_engine=debug,network=trace")
    pub level: String,
    /// Output format: "pretty" for human-readable, "json" for structured JSON logs
    pub format: LogFormat,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogFormat {
    Pretty,
    Json,
}

/// Telemetry configuration.
///
/// This explicitly separates:
/// - traces: request/operation timelines across components
/// - metrics: numeric time series for dashboards/alerts
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct TelemetryConfig {
    /// Trace export configuration.
    pub traces: TelemetryTracesConfig,
    /// Metrics exporter configuration.
    pub metrics: TelemetryMetricsConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct TelemetryMetricsConfig {
    /// Whether to expose Prometheus metrics.
    pub enabled: bool,
    /// Bind address for Prometheus metrics endpoint.
    pub bind_address: String,
}

/// OpenTelemetry trace export configuration.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct TelemetryTracesConfig {
    /// Whether to enable trace export.
    pub enabled: bool,
    /// OTLP endpoint for traces (e.g., "http://tempo:4317").
    pub otlp_endpoint: String,
    /// Service name reported in traces.
    pub service_name: String,
}
