//! Fetch stage: retrieves KCs from network peers.
//!
//! Receives filtered KCs from the filter stage, fetches data from network peers
//! using FuturesUnordered for concurrent requests, validates responses, and
//! sends fetched data to the insert stage.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use dkg_blockchain::BlockchainId;
use dkg_domain::{ParsedUal, TokenIds, Visibility, parse_ual};
use dkg_network::{
    BatchGetRequestData, BatchGetResponseData, NetworkError, NetworkManager, PeerId,
    STREAM_PROTOCOL_BATCH_GET,
};
use dkg_observability as observability;
use futures::{StreamExt, stream::FuturesUnordered};
use tokio::sync::mpsc;
use tracing::instrument;

use super::types::{FetchedKc, KcToSync, QueueOutcome};
use crate::{
    application::{AssertionValidation, UAL_MAX_LIMIT},
    node_state::PeerRegistry,
};

/// Maximum number of in-flight peer requests for this operation.
/// For very large KC batches, use a single in-flight peer request to reduce peak memory.
const LARGE_BATCH_ASSET_THRESHOLD_FOR_SINGLE_PEER: u64 = 1_000;

/// Fetch stage: receives filtered KCs, fetches from network, sends to insert stage.
#[allow(clippy::too_many_arguments)]
#[instrument(
    name = "sync_fetch",
    skip(
        rx,
        network_manager,
        assertion_validation,
        peer_registry,
        tx,
        outcome_tx
    ),
    fields(blockchain_id = %blockchain_id)
)]
pub(crate) async fn run_fetch_stage(
    mut rx: mpsc::Receiver<Vec<KcToSync>>,
    network_fetch_batch_size: usize,
    batch_get_fanout_concurrency: usize,
    max_assets_per_fetch_batch: u64,
    blockchain_id: BlockchainId,
    network_manager: Arc<NetworkManager>,
    assertion_validation: Arc<AssertionValidation>,
    peer_registry: Arc<PeerRegistry>,
    tx: mpsc::Sender<Vec<FetchedKc>>,
    outcome_tx: mpsc::Sender<Vec<QueueOutcome>>,
) {
    let task_start = Instant::now();
    let network_fetch_batch_size = network_fetch_batch_size.max(1);
    let batch_get_fanout_concurrency = batch_get_fanout_concurrency.max(1);
    let max_assets_per_fetch_batch = max_assets_per_fetch_batch.max(1);
    let mut total_failures = 0usize;
    let mut total_fetched = 0usize;
    let mut total_received = 0usize;

    // Accumulate KCs until we have enough for a network batch
    let mut accumulated: Vec<KcToSync> = Vec::new();

    while let Some(batch) = rx.recv().await {
        total_received += batch.len();
        accumulated.extend(batch);

        // Process when we have enough KCs to start fetching, or when channel is empty
        while accumulated.len() >= network_fetch_batch_size
            || (!accumulated.is_empty() && rx.is_empty())
        {
            let mut assets_total = 0u64;
            let mut take = 0usize;
            for kc in &accumulated {
                if take >= UAL_MAX_LIMIT {
                    break;
                }
                let kc_assets = estimate_asset_count(&kc.token_ids);
                if take > 0 && assets_total.saturating_add(kc_assets) > max_assets_per_fetch_batch {
                    break;
                }
                assets_total = assets_total.saturating_add(kc_assets);
                take += 1;
            }

            if take == 0 && !accumulated.is_empty() {
                take = 1;
                assets_total = estimate_asset_count(&accumulated[0].token_ids);
            }

            let to_fetch: Vec<KcToSync> = accumulated.drain(..take).collect();

            let fetch_start = Instant::now();
            let (fetched, batch_failures) = fetch_kc_batch_with_live_peers(
                &blockchain_id,
                &to_fetch,
                batch_get_fanout_concurrency,
                &network_manager,
                &assertion_validation,
                &peer_registry,
            )
            .await;
            let batch_status = if batch_failures.is_empty() {
                "success"
            } else if fetched.is_empty() {
                "failed"
            } else {
                "partial"
            };
            observability::record_sync_fetch_batch(
                batch_status,
                fetch_start.elapsed(),
                to_fetch.len(),
                assets_total,
            );

            total_fetched += fetched.len();
            total_failures += batch_failures.len();
            observability::record_sync_kc_outcome(
                blockchain_id.as_str(),
                "fetch",
                "failed",
                batch_failures.len(),
            );
            if !batch_failures.is_empty() && outcome_tx.send(batch_failures).await.is_err() {
                return;
            }

            // Send fetched KCs to insert stage
            if !fetched.is_empty() {
                tracing::trace!(
                    batch_size = to_fetch.len(),
                    assets_in_batch = assets_total,
                    fetched_count = fetched.len(),
                    total_failures,
                    fetch_ms = fetch_start.elapsed().as_millis() as u64,
                    total_received,
                    total_fetched,
                    elapsed_ms = task_start.elapsed().as_millis() as u64,
                    "Fetch: sending batch to insert stage"
                );
                if tx.send(fetched).await.is_err() {
                    tracing::trace!("Fetch: insert stage receiver dropped, stopping");
                    let remaining_outcomes: Vec<QueueOutcome> = accumulated
                        .drain(..)
                        .map(|kc| QueueOutcome::retry(kc.contract_addr_str, kc.kc_id))
                        .collect();
                    if !remaining_outcomes.is_empty() {
                        let _ = outcome_tx.send(remaining_outcomes).await;
                    }
                    return;
                }
            }

            // Break inner loop if channel is empty to avoid busy-waiting
            if rx.is_empty() {
                break;
            }
        }
    }

    // Flush any remaining accumulated KCs
    if !accumulated.is_empty() {
        let fetch_start = Instant::now();
        let assets_total: u64 = accumulated
            .iter()
            .map(|kc| estimate_asset_count(&kc.token_ids))
            .sum();
        let (fetched, batch_failures) = fetch_kc_batch_with_live_peers(
            &blockchain_id,
            &accumulated,
            batch_get_fanout_concurrency,
            &network_manager,
            &assertion_validation,
            &peer_registry,
        )
        .await;
        let batch_status = if batch_failures.is_empty() {
            "success"
        } else if fetched.is_empty() {
            "failed"
        } else {
            "partial"
        };
        observability::record_sync_fetch_batch(
            batch_status,
            fetch_start.elapsed(),
            accumulated.len(),
            assets_total,
        );

        total_fetched += fetched.len();
        total_failures += batch_failures.len();
        observability::record_sync_kc_outcome(
            blockchain_id.as_str(),
            "fetch",
            "failed",
            batch_failures.len(),
        );
        if !batch_failures.is_empty() {
            let _ = outcome_tx.send(batch_failures).await;
        }

        if !fetched.is_empty() {
            tracing::trace!(
                remaining = accumulated.len(),
                assets_in_batch = assets_total,
                fetched_count = fetched.len(),
                total_failures,
                fetch_ms = fetch_start.elapsed().as_millis() as u64,
                "Fetch: flushing remaining KCs"
            );
            let _ = tx.send(fetched).await;
        }
    }

    tracing::debug!(
        total_ms = task_start.elapsed().as_millis() as u64,
        total_fetched,
        failures = total_failures,
        "Fetch stage completed"
    );
}

fn estimate_asset_count(token_ids: &TokenIds) -> u64 {
    let start = token_ids.start_token_id();
    let end = token_ids.end_token_id();
    let range = end.saturating_sub(start).saturating_add(1);
    let burned = token_ids.burned().len() as u64;
    range.saturating_sub(burned)
}

async fn fetch_kc_batch_with_live_peers(
    blockchain_id: &BlockchainId,
    kcs: &[KcToSync],
    batch_get_fanout_concurrency: usize,
    network_manager: &Arc<NetworkManager>,
    assertion_validation: &AssertionValidation,
    peer_registry: &PeerRegistry,
) -> (Vec<FetchedKc>, Vec<QueueOutcome>) {
    let (mut peers, peer_stats) = get_shard_peers(blockchain_id, network_manager, peer_registry);
    if peers.is_empty() {
        let identified_peers = peer_registry.identified_shard_peer_count(blockchain_id);
        tracing::debug!(
            blockchain_id = %blockchain_id,
            batch_kcs = kcs.len(),
            shard_members = peer_stats.shard_members,
            identified = identified_peers,
            usable = peer_stats.usable,
            "Fetch: no peers available for batch"
        );
        let failures = kcs
            .iter()
            .map(|kc| QueueOutcome::retry(kc.contract_addr_str.clone(), kc.kc_id))
            .collect();
        return (Vec::new(), failures);
    }

    peer_registry.sort_by_latency(&mut peers);
    fetch_kc_batch_from_network(
        blockchain_id,
        kcs,
        &peers,
        batch_get_fanout_concurrency,
        network_manager,
        assertion_validation,
    )
    .await
}

/// Get shard peers for the given blockchain, excluding self.
fn get_shard_peers(
    blockchain_id: &BlockchainId,
    network_manager: &NetworkManager,
    peer_registry: &PeerRegistry,
) -> (std::vec::Vec<dkg_network::PeerId>, PeerSelectionStats) {
    let my_peer_id = network_manager.peer_id();

    // Get shard peers that support BatchGetProtocol, excluding self
    let peers = peer_registry.select_shard_peers(
        blockchain_id,
        STREAM_PROTOCOL_BATCH_GET,
        Some(my_peer_id),
    );

    // Stats: total in shard vs usable (have identify + support protocol)
    let shard_peer_count = peer_registry.shard_peer_count(blockchain_id);

    let stats = PeerSelectionStats {
        shard_members: shard_peer_count,
        usable: peers.len(),
    };

    (peers, stats)
}

#[derive(Clone, Copy, Debug)]
struct PeerSelectionStats {
    /// Total peers in shard (including self)
    shard_members: usize,
    /// Peers that passed all filters (have identify, support protocol, not self)
    usable: usize,
}

/// Helper function for making peer requests.
/// Using a named async fn instead of async blocks allows FuturesUnordered
/// to work without boxing, since all calls return the same concrete future type.
async fn make_peer_request(
    peer: PeerId,
    request_data: BatchGetRequestData,
    nm: Arc<NetworkManager>,
) -> (PeerId, Result<BatchGetResponseData, NetworkError>, Duration) {
    let start = Instant::now();
    let result = nm
        .send_batch_get_request(peer, uuid::Uuid::new_v4(), request_data)
        .await;
    (peer, result, start.elapsed())
}

/// Fetch a batch of KCs from the network.
/// Returns (fetched KCs, failed KCs).
///
/// Uses FuturesUnordered to process peer responses as they arrive,
/// avoiding being blocked by slow/unreachable peers.
/// Peers should be pre-sorted by performance score (best first).
#[instrument(
    name = "sync_fetch_batch",
    skip(kcs, peers, network_manager, assertion_validation),
    fields(
        blockchain_id = %blockchain_id,
        kc_count = kcs.len(),
        peer_count = peers.len(),
        fetched = tracing::field::Empty,
        failed = tracing::field::Empty,
    )
)]
async fn fetch_kc_batch_from_network(
    blockchain_id: &BlockchainId,
    kcs: &[KcToSync],
    peers: &[PeerId],
    batch_get_fanout_concurrency: usize,
    network_manager: &Arc<NetworkManager>,
    assertion_validation: &AssertionValidation,
) -> (Vec<FetchedKc>, Vec<QueueOutcome>) {
    let mut fetched: Vec<FetchedKc> = Vec::new();
    let mut uals_still_needed: HashSet<String> = kcs.iter().map(|kc| kc.ual.clone()).collect();
    let estimated_assets: u64 = kcs
        .iter()
        .map(|kc| estimate_asset_count(&kc.token_ids))
        .sum();
    let concurrent_peers = if estimated_assets >= LARGE_BATCH_ASSET_THRESHOLD_FOR_SINGLE_PEER {
        1
    } else {
        batch_get_fanout_concurrency.max(1)
    };

    // Build lookup maps
    let ual_to_kc: HashMap<&str, &KcToSync> = kcs.iter().map(|kc| (kc.ual.as_str(), kc)).collect();

    // Parse UALs for validation
    let parsed_uals: HashMap<String, ParsedUal> = kcs
        .iter()
        .filter_map(|kc| {
            parse_ual(&kc.ual)
                .ok()
                .map(|parsed| (kc.ual.clone(), parsed))
        })
        .collect();

    // Build token IDs map for the request
    let token_ids_map: HashMap<String, TokenIds> = kcs
        .iter()
        .map(|kc| (kc.ual.clone(), kc.token_ids.clone()))
        .collect();

    // Helper to build batch request for remaining UALs
    let build_request = |uals: &HashSet<String>| {
        let filtered_token_ids: HashMap<String, TokenIds> = token_ids_map
            .iter()
            .filter(|(ual, _)| uals.contains(*ual))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        BatchGetRequestData::new(
            blockchain_id.to_string(),
            uals.iter().cloned().collect(),
            filtered_token_ids,
            false, // include_metadata
        )
    };

    // Use FuturesUnordered to process responses as they arrive
    let mut futures = FuturesUnordered::new();
    let mut peers_iter = peers.iter();

    // Build initial request with all UALs
    let initial_request = build_request(&uals_still_needed);

    // Start initial batch of concurrent requests
    tracing::trace!(
        kc_count = kcs.len(),
        estimated_assets,
        concurrent_peers,
        "Fetch: selected peer request concurrency for batch"
    );

    for _ in 0..concurrent_peers {
        if let Some(&peer) = peers_iter.next() {
            futures.push(make_peer_request(
                peer,
                initial_request.clone(),
                Arc::clone(network_manager),
            ));
        }
    }

    // Process responses as they complete, adding new peers as slots free up
    while let Some((peer, result, elapsed)) = futures.next().await {
        // Create a span for this peer's response
        let peer_span = tracing::trace_span!(
            "peer_response",
            peer_id = %peer,
            latency_ms = elapsed.as_millis() as u64,
            valid_kcs = tracing::field::Empty,
            status = tracing::field::Empty,
        );
        let _guard = peer_span.enter();

        match result {
            Ok(BatchGetResponseData::Ack(mut ack)) => {
                let mut valid_count = 0usize;

                for (ual, assertion) in ack.assertions.drain() {
                    if !uals_still_needed.contains(&ual) {
                        continue;
                    }

                    if !assertion.has_data() {
                        continue;
                    }

                    if let (Some(parsed_ual), Some(kc)) =
                        (parsed_uals.get(&ual), ual_to_kc.get(ual.as_str()))
                    {
                        // Use pre-fetched merkle root from filter stage (no RPC call needed)
                        let is_valid = assertion_validation.validate_response_with_root(
                            &assertion,
                            parsed_ual,
                            Visibility::All,
                            kc.merkle_root.as_deref(),
                        );

                        if is_valid {
                            uals_still_needed.remove(&ual);
                            fetched.push(FetchedKc {
                                contract_addr_str: kc.contract_addr_str.clone(),
                                kc_id: kc.kc_id,
                                ual,
                                assertion,
                                metadata: Some(kc.metadata.clone()),
                                estimated_assets: estimate_asset_count(&kc.token_ids),
                            });
                            valid_count += 1;
                        }
                    }
                }

                peer_span.record("valid_kcs", valid_count);
                peer_span.record("status", tracing::field::display("ok"));

                // Break early if we got everything - don't wait for remaining in-flight requests
                if uals_still_needed.is_empty() {
                    break;
                }
            }
            Ok(BatchGetResponseData::Error(_)) => {
                peer_span.record("valid_kcs", 0usize);
                peer_span.record("status", tracing::field::display("nack"));
            }
            Err(e) => {
                let status = e.to_string();
                peer_span.record("valid_kcs", 0usize);
                peer_span.record("status", tracing::field::display(&status));
            }
        }

        // Add next peer if we still need KCs and have more peers available
        if !uals_still_needed.is_empty()
            && let Some(&next_peer) = peers_iter.next()
        {
            let updated_request = build_request(&uals_still_needed);
            futures.push(make_peer_request(
                next_peer,
                updated_request,
                Arc::clone(network_manager),
            ));
        }
    }

    // Map remaining UALs back to queue outcomes.
    let failed: Vec<QueueOutcome> = uals_still_needed
        .iter()
        .filter_map(|ual| {
            ual_to_kc
                .get(ual.as_str())
                .map(|kc| QueueOutcome::retry(kc.contract_addr_str.clone(), kc.kc_id))
        })
        .collect();

    tracing::Span::current().record("fetched", fetched.len());
    tracing::Span::current().record("failed", failed.len());

    (fetched, failed)
}
