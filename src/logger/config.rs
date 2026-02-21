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
/// This currently includes only metrics:
/// - metrics: numeric time series for dashboards/alerts
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct TelemetryConfig {
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
