//! DKG Sync Pipeline
//!
//! This module runs split sync workers:
//! - `MetadataSyncTask` (producer): backfills KC events, hydrates chain state, enqueues IDs
//! - `SyncTask` (consumer): drains queue and performs filter/fetch/insert

pub(crate) mod burned_encoding;
mod config;
mod fetch;
mod filter;
mod insert;
mod metadata_task;
mod task;
mod types;

// Re-export public types
pub(crate) use config::SyncConfig;
pub(crate) use metadata_task::MetadataSyncTask;
pub(crate) use task::SyncTask;
