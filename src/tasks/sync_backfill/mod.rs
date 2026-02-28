//! Unified sync backfill task.
//!
//! This module runs one bounded backfill task composed of:
//! - Replenisher: backfills KC events, hydrates chain state, enqueues IDs
//! - Dispatcher + stages: drains queue and performs filter/fetch/insert

mod config;
mod fetch;
mod filter;
mod insert;
mod metadata_replenisher;
mod pipeline;
mod task;
mod types;

// Re-export public types
pub(crate) use config::SyncConfig;
pub(crate) use task::SyncBackfillTask;
