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

mod fetch;
mod filter;
mod insert;
mod task;
mod types;

use std::time::Duration;

// Re-export public types
pub(crate) use task::SyncTask;

/// Interval between sync cycles when there's pending work (catching up)
pub(crate) const SYNC_PERIOD_CATCHING_UP: Duration = Duration::from_secs(0);

/// Interval between sync cycles when caught up (idle polling for new KCs)
pub(crate) const SYNC_PERIOD_IDLE: Duration = Duration::from_secs(30);

/// Maximum retry attempts before a KC is no longer retried (stays in DB for future recovery)
pub(crate) const MAX_RETRY_ATTEMPTS: u32 = 2;

/// Maximum new KCs to process per contract per sync cycle
pub(crate) const MAX_NEW_KCS_PER_CONTRACT: u64 = 1000;

/// Batch size for filter task (KCs per batch sent through channel)
/// Aligned with MULTICALL_CHUNK_SIZE (100) for optimal RPC batching.
pub(crate) const FILTER_BATCH_SIZE: usize = 100;

/// Batch size for network fetch (start fetching when we have this many KCs).
/// Set to match FILTER_BATCH_SIZE to start fetching as soon as first filter batch completes.
/// This enables true pipeline overlap: fetch starts while filter is still processing.
pub(crate) const NETWORK_FETCH_BATCH_SIZE: usize = 100;

/// Maximum number of knowledge assets to include in a single network fetch batch.
/// This limits payload size when KC sizes vary widely.
pub(crate) const MAX_ASSETS_PER_FETCH_BATCH: u64 = 10_000;

/// Channel buffer size (number of batches that can be buffered between stages)
pub(crate) const PIPELINE_CHANNEL_BUFFER: usize = 6;
