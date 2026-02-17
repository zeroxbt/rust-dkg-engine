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
    NetworkError, NetworkManager, PeerId, STREAM_PROTOCOL_BATCH_GET,
    messages::{BatchGetRequestData, BatchGetResponseData},
};
use dkg_triple_store::parse_metadata_from_triples;
use futures::{StreamExt, stream::FuturesUnordered};
use tokio::sync::mpsc;
use tracing::instrument;

use super::types::{FetchStats, FetchedKc, KcToSync};
use crate::{
    commands::operations::batch_get::handle_batch_get_request::UAL_MAX_LIMIT,
    services::{AssertionValidationService, PeerService},
};

/// Maximum number of in-flight peer requests for this operation.
pub(crate) const CONCURRENT_PEERS: usize = 3;

/// Fetch task: receives filtered KCs, fetches from network, sends to insert stage.
#[instrument(
    name = "sync_fetch",
    skip(
        rx,
        network_manager,
        assertion_validation_service,
        peer_service,
        tx
    ),
    fields(blockchain_id = %blockchain_id)
)]
pub(crate) async fn fetch_task(
    mut rx: mpsc::Receiver<Vec<KcToSync>>,
    network_fetch_batch_size: usize,
    max_assets_per_fetch_batch: u64,
    blockchain_id: BlockchainId,
    network_manager: Arc<NetworkManager>,
    assertion_validation_service: Arc<AssertionValidationService>,
    peer_service: Arc<PeerService>,
    tx: mpsc::Sender<Vec<FetchedKc>>,
) -> FetchStats {
    let task_start = Instant::now();
    let network_fetch_batch_size = network_fetch_batch_size.max(1);
    let max_assets_per_fetch_batch = max_assets_per_fetch_batch.max(1);
    let mut failures: Vec<u64> = Vec::new();
    let mut total_fetched = 0usize;
    let mut total_received = 0usize;

    // Get shard peers once at the start, sorted by performance score
    let (mut peers, peer_stats) = get_shard_peers(&blockchain_id, &network_manager, &peer_service);

    if peers.is_empty() {
        let identified = peer_service.identified_shard_peer_count(&blockchain_id);
        tracing::warn!(
            blockchain_id = %blockchain_id,
            shard_members = peer_stats.shard_members,
            identified,
            usable = peer_stats.usable,
            "Fetch: no peers available"
        );
        while let Some(batch) = rx.recv().await {
            failures.extend(batch.iter().map(|kc| kc.kc_id));
        }
        return FetchStats { failures };
    }

    // Sort peers by latency (fastest first)
    peer_service.sort_by_latency(&mut peers);
    if tracing::enabled!(tracing::Level::DEBUG) {
        const MAX_PEERS_TO_LOG: usize = 20;
        let peer_latencies: Vec<String> = peers
            .iter()
            .take(MAX_PEERS_TO_LOG)
            .map(|peer| {
                let latency = peer_service.get_average_latency(peer);
                format!("{peer}:{latency}ms")
            })
            .collect();

        tracing::debug!(
            blockchain_id = %blockchain_id,
            peer_count = peers.len(),
            displayed = peer_latencies.len(),
            truncated = peers.len() > MAX_PEERS_TO_LOG,
            peers = ?peer_latencies,
            "Fetch: peer latency order"
        );
    }

    let min_required_peers = 1;
    if peers.len() < min_required_peers {
        let identified = peer_service.identified_shard_peer_count(&blockchain_id);
        tracing::warn!(
            blockchain_id = %blockchain_id,
            found = peers.len(),
            required = min_required_peers,
            shard_members = peer_stats.shard_members,
            identified,
            usable = peer_stats.usable,
            "Fetch: not enough peers available"
        );
        while let Some(batch) = rx.recv().await {
            failures.extend(batch.iter().map(|kc| kc.kc_id));
        }
        return FetchStats { failures };
    }

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
            let (fetched, batch_failures) = fetch_kc_batch_from_network(
                &blockchain_id,
                &to_fetch,
                &peers,
                &network_manager,
                &assertion_validation_service,
            )
            .await;

            total_fetched += fetched.len();
            failures.extend(batch_failures);

            // Send fetched KCs to insert stage
            if !fetched.is_empty() {
                tracing::debug!(
                    blockchain_id = %blockchain_id,
                    batch_size = to_fetch.len(),
                    assets_in_batch = assets_total,
                    fetched_count = fetched.len(),
                    failed_count = failures.len(),
                    fetch_ms = fetch_start.elapsed().as_millis() as u64,
                    total_received,
                    total_fetched,
                    elapsed_ms = task_start.elapsed().as_millis() as u64,
                    "Fetch: sending batch to insert stage"
                );
                if tx.send(fetched).await.is_err() {
                    tracing::debug!("Fetch: insert stage receiver dropped, stopping");
                    failures.extend(accumulated.iter().map(|kc| kc.kc_id));
                    return FetchStats { failures };
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
        let (fetched, batch_failures) = fetch_kc_batch_from_network(
            &blockchain_id,
            &accumulated,
            &peers,
            &network_manager,
            assertion_validation_service.as_ref(),
        )
        .await;

        total_fetched += fetched.len();
        failures.extend(batch_failures);

        if !fetched.is_empty() {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                remaining = accumulated.len(),
                assets_in_batch = assets_total,
                fetched_count = fetched.len(),
                final_failures = failures.len(),
                fetch_ms = fetch_start.elapsed().as_millis() as u64,
                "Fetch: flushing remaining KCs"
            );
            let _ = tx.send(fetched).await;
        }
    }

    tracing::debug!(
        blockchain_id = %blockchain_id,
        total_ms = task_start.elapsed().as_millis() as u64,
        total_fetched,
        failures = failures.len(),
        "Fetch task completed"
    );

    FetchStats { failures }
}

fn estimate_asset_count(token_ids: &TokenIds) -> u64 {
    let start = token_ids.start_token_id();
    let end = token_ids.end_token_id();
    let range = end.saturating_sub(start).saturating_add(1);
    let burned = token_ids.burned().len() as u64;
    range.saturating_sub(burned)
}

/// Get shard peers for the given blockchain, excluding self.
fn get_shard_peers(
    blockchain_id: &BlockchainId,
    network_manager: &NetworkManager,
    peer_service: &PeerService,
) -> (std::vec::Vec<dkg_network::PeerId>, PeerSelectionStats) {
    let my_peer_id = network_manager.peer_id();

    // Get shard peers that support BatchGetProtocol, excluding self
    let peers =
        peer_service.select_shard_peers(blockchain_id, STREAM_PROTOCOL_BATCH_GET, Some(my_peer_id));

    // Stats: total in shard vs usable (have identify + support protocol)
    let shard_peer_count = peer_service.shard_peer_count(blockchain_id);

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
/// Returns (fetched KCs, failed KC IDs).
///
/// Uses FuturesUnordered to process peer responses as they arrive,
/// avoiding being blocked by slow/unreachable peers.
/// Peers should be pre-sorted by performance score (best first).
#[instrument(
    name = "sync_fetch_batch",
    skip(kcs, peers, network_manager, assertion_validation_service),
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
    network_manager: &Arc<NetworkManager>,
    assertion_validation_service: &AssertionValidationService,
) -> (Vec<FetchedKc>, Vec<u64>) {
    let mut fetched: Vec<FetchedKc> = Vec::new();
    let mut uals_still_needed: HashSet<String> = kcs.iter().map(|kc| kc.ual.clone()).collect();

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
            true, // include_metadata
        )
    };

    // Use FuturesUnordered to process responses as they arrive
    let mut futures = FuturesUnordered::new();
    let mut peers_iter = peers.iter();

    // Build initial request with all UALs
    let initial_request = build_request(&uals_still_needed);

    // Start initial batch of concurrent requests
    for _ in 0..CONCURRENT_PEERS {
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
        let peer_span = tracing::debug_span!(
            "peer_response",
            peer_id = %peer,
            latency_ms = elapsed.as_millis() as u64,
            valid_kcs = tracing::field::Empty,
            status = tracing::field::Empty,
        );
        let _guard = peer_span.enter();

        match result {
            Ok(BatchGetResponseData::Ack(ack)) => {
                let assertions = &ack.assertions;
                let metadata_map = &ack.metadata;
                let mut valid_count = 0usize;

                for (ual, assertion) in assertions {
                    if !uals_still_needed.contains(ual) {
                        continue;
                    }

                    if !assertion.has_data() {
                        continue;
                    }

                    if let (Some(parsed_ual), Some(kc)) =
                        (parsed_uals.get(ual), ual_to_kc.get(ual.as_str()))
                    {
                        // Use pre-fetched merkle root from filter stage (no RPC call needed)
                        let is_valid = assertion_validation_service.validate_response_with_root(
                            assertion,
                            parsed_ual,
                            Visibility::All,
                            kc.merkle_root.as_deref(),
                        );

                        if is_valid {
                            let metadata = metadata_map
                                .get(ual)
                                .and_then(|triples| parse_metadata_from_triples(triples));

                            fetched.push(FetchedKc {
                                kc_id: kc.kc_id,
                                ual: ual.clone(),
                                assertion: assertion.clone(),
                                metadata,
                            });
                            uals_still_needed.remove(ual);
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

    // Map remaining UALs back to KC IDs for the failed list
    let failed_kc_ids: Vec<u64> = uals_still_needed
        .iter()
        .filter_map(|ual| ual_to_kc.get(ual.as_str()).map(|kc| kc.kc_id))
        .collect();

    // Record results in span
    tracing::Span::current().record("fetched", fetched.len());
    tracing::Span::current().record("failed", failed_kc_ids.len());

    (fetched, failed_kc_ids)
}
