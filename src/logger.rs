//! Logger initialization module.
//!
//! Configures tracing-based logging with support for:
//! - Pretty (human-readable) or JSON output formats
//! - Configurable log levels via config file or RUST_LOG env var
//! - Environment variable override (RUST_LOG takes precedence)

use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use crate::config::{LogFormat, LoggerConfig};

/// Initialize the global logger with the given configuration.
///
/// The `RUST_LOG` environment variable takes precedence over the config file setting.
/// If neither is set, defaults to `rust_ot_node=info`.
pub(crate) fn initialize(config: &LoggerConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));

    let subscriber = tracing_subscriber::registry().with(filter);

    match config.format {
        LogFormat::Pretty => {
            subscriber
                .with(
                    fmt::layer()
                        .with_target(true)
                        .with_thread_ids(false)
                        .with_file(false)
                        .with_line_number(false),
                )
                .init();
        }
        LogFormat::Json => {
            subscriber.with(fmt::layer().json()).init();
        }
    }
}
