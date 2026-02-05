use serde::Deserialize;

/// Logger configuration for tracing output.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct LoggerConfig {
    /// Log level filter (e.g., "info", "debug", "trace", or module-specific like
    /// "rust_ot_node=debug,network=trace")
    pub level: String,
    /// Output format: "pretty" for human-readable, "json" for structured JSON logs
    pub format: LogFormat,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogFormat {
    Pretty,
    Json,
}

/// OpenTelemetry configuration for distributed tracing.
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub(crate) struct TelemetryConfig {
    /// Whether to enable OpenTelemetry tracing export
    pub enabled: bool,
    /// OTLP endpoint for trace export (e.g., "http://tempo:4317")
    pub otlp_endpoint: String,
    /// Service name reported in traces
    pub service_name: String,
}
