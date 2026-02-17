//! DKG Sync Pipeline
//!
//! This module implements a three-stage pipeline for syncing Knowledge Collections:
//!
//! ```text
//! Filter Stage              Fetch Stage           Insert Stage
//! ├─ Local existence        ├─ Network requests   └─ Triple store insert
//! ├─ Single Multicall RPC   └─ Validation
//! │  (epochs, ranges, roots)
//! ├─ Filter expired
//! └─ Send to fetch ───────→ Send to insert ─────→
//! ```
//!
//! The pipeline allows stages to overlap, reducing total sync time.
//! All RPC calls are batched into a single Multicall per filter batch.

mod config;
mod fetch;
mod filter;
mod insert;
mod task;
mod types;

// Re-export public types
pub(crate) use config::SyncConfig;
pub(crate) use task::SyncTask;
