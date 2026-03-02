//! Sync queue processor with optional discovery worker.
//!
//! This module runs one long-lived queue processor composed of:
//! - Discovery: scans KC events, hydrates chain state, enqueues IDs
//! - Dispatcher + stages: drains queue and performs filter/fetch/insert

mod config;
mod discovery;
mod pipeline;
mod queue;
mod task;

// Re-export public types
pub(crate) use config::{DkgSyncConfig, DkgSyncDiscoveryConfig, DkgSyncQueueProcessorConfig};
pub(crate) use task::DkgSyncTask;
