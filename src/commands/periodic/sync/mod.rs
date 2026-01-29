//! DKG Sync Pipeline
//!
//! This module implements a three-stage pipeline for syncing Knowledge Collections:
//!
//! ```text
//! Filter Stage              Fetch Stage           Insert Stage
//! ├─ Local existence        ├─ Network requests   └─ Triple store insert
//! ├─ RPC (end epochs)       └─ Validation
//! ├─ Filter expired
//! ├─ RPC (token ranges)
//! └─ Send to fetch ───────→ Send to insert ─────→
//! ```
//!
//! The pipeline allows stages to overlap, reducing total sync time.
//! Expiration filtering is done early in the filter stage to avoid
//! fetching data for expired KCs.

mod fetch;
mod filter;
mod insert;
mod types;

use std::time::Duration;

pub(crate) use fetch::fetch_task;
pub(crate) use filter::filter_task;
pub(crate) use insert::insert_task;
pub(crate) use types::*;

use crate::{
    commands::operations::get::protocols::batch_get::BATCH_GET_UAL_MAX_LIMIT,
    managers::triple_store::{
        KnowledgeCollectionMetadata,
        query::predicates,
        rdf::{
            extract_datetime_as_unix, extract_quoted_integer, extract_quoted_string,
            extract_uri_suffix,
        },
    },
};

/// Interval between sync cycles
pub(crate) const SYNC_PERIOD: Duration = Duration::from_secs(0);

/// Maximum retry attempts before a KC is no longer retried (stays in DB for future recovery)
pub(crate) const MAX_RETRY_ATTEMPTS: u32 = 2;

/// Maximum new KCs to process per contract per sync cycle
pub(crate) const MAX_NEW_KCS_PER_CONTRACT: u64 =
    BATCH_GET_UAL_MAX_LIMIT as u64 / MAX_RETRY_ATTEMPTS as u64;

/// Batch size for filter task (KCs per batch sent through channel)
/// Aligned with MULTICALL_CHUNK_SIZE (100) for optimal RPC batching.
pub(crate) const FILTER_BATCH_SIZE: usize = 100;

/// Batch size for network fetch (start fetching when we have this many KCs).
/// Set to match FILTER_BATCH_SIZE to start fetching as soon as first filter batch completes.
/// This enables true pipeline overlap: fetch starts while filter is still processing.
pub(crate) const NETWORK_FETCH_BATCH_SIZE: usize = 50;

/// Channel buffer size (number of batches that can be buffered between stages)
pub(crate) const PIPELINE_CHANNEL_BUFFER: usize = 3;

/// Number of peers to query concurrently during network fetch
pub(crate) const CONCURRENT_PEER_REQUESTS: usize = 3;

/// Parse metadata from network RDF triples.
///
/// Returns KnowledgeCollectionMetadata if all required fields are found, None otherwise.
pub(crate) fn parse_metadata_from_triples(
    triples: &[String],
) -> Option<KnowledgeCollectionMetadata> {
    let mut publisher_address: Option<String> = None;
    let mut block_number: Option<u64> = None;
    let mut transaction_hash: Option<String> = None;
    let mut block_timestamp: Option<u64> = None;

    for triple in triples {
        if triple.contains(predicates::PUBLISHED_BY) {
            publisher_address = extract_uri_suffix(triple, predicates::PUBLISHER_KEY_PREFIX);
        } else if triple.contains(predicates::PUBLISHED_AT_BLOCK) {
            block_number = extract_quoted_integer(triple);
        } else if triple.contains(predicates::PUBLISH_TX) {
            transaction_hash = extract_quoted_string(triple);
        } else if triple.contains(predicates::BLOCK_TIME) {
            block_timestamp = extract_datetime_as_unix(triple);
        }
    }

    match (
        publisher_address,
        block_number,
        transaction_hash,
        block_timestamp,
    ) {
        (Some(publisher), Some(block), Some(tx_hash), Some(timestamp)) => Some(
            KnowledgeCollectionMetadata::new(publisher.to_lowercase(), block, tx_hash, timestamp),
        ),
        _ => None,
    }
}
