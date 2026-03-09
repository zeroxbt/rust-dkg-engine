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

use crate::{
    application::{AssertionValidation, UAL_MAX_LIMIT},
    node_state::PeerRegistry,
    tasks::dkg_sync::pipeline::types::{FetchedKc, KcToSync, QueueKcKey, QueueOutcome},
};

const FETCH_STAGE_FAILURE_REASON: &str = "fetch_stage_failure";

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
    fetch_max_kc_per_batch: usize,
    fetch_batch_concurrency: usize,
    batch_get_fanout_concurrency: usize,
    max_peer_attempts_per_batch: Option<usize>,
    fetch_max_ka_per_batch: u64,
    blockchain_id: BlockchainId,
    network_manager: Arc<NetworkManager>,
    assertion_validation: Arc<AssertionValidation>,
    peer_registry: Arc<PeerRegistry>,
    tx: mpsc::Sender<Vec<FetchedKc>>,
    outcome_tx: mpsc::Sender<Vec<QueueOutcome>>,
) {
    let fetch_max_kc_per_batch = fetch_max_kc_per_batch.max(1);
    let fetch_batch_concurrency = fetch_batch_concurrency.max(1);
    let batch_get_fanout_concurrency = batch_get_fanout_concurrency.max(1);
    let max_peer_attempts_per_batch = max_peer_attempts_per_batch.filter(|value| *value > 0);
    let fetch_max_ka_per_batch = fetch_max_ka_per_batch.max(1);

    let mut accumulated: Vec<KcToSync> = Vec::new();
    let mut rx_closed = false;
    let mut next_batch_id = 0u64;
    let mut in_flight = FuturesUnordered::new();
    let mut in_flight_keys_by_batch: HashMap<u64, Vec<QueueKcKey>> = HashMap::new();

    loop {
        while in_flight.len() < fetch_batch_concurrency {
            let allow_partial_batch = rx_closed || rx.is_empty();
            let Some((to_fetch, assets_total)) = take_next_fetch_batch(
                &mut accumulated,
                fetch_max_kc_per_batch,
                fetch_max_ka_per_batch,
                allow_partial_batch,
            ) else {
                break;
            };

            let batch_id = next_batch_id;
            next_batch_id = next_batch_id.saturating_add(1);
            in_flight_keys_by_batch
                .insert(batch_id, to_fetch.iter().map(|kc| kc.key.clone()).collect());

            let blockchain_id_for_batch = blockchain_id.clone();
            let network_manager_for_batch = Arc::clone(&network_manager);
            let assertion_validation_for_batch = Arc::clone(&assertion_validation);
            let peer_registry_for_batch = Arc::clone(&peer_registry);
            in_flight.push(async move {
                let fetch_start = Instant::now();
                let (fetched, failures) = fetch_kc_batch_with_live_peers(
                    &blockchain_id_for_batch,
                    &to_fetch,
                    batch_get_fanout_concurrency,
                    max_peer_attempts_per_batch,
                    &network_manager_for_batch,
                    assertion_validation_for_batch.as_ref(),
                    peer_registry_for_batch.as_ref(),
                )
                .await;
                FetchBatchOutput {
                    batch_id,
                    batch_size: to_fetch.len(),
                    assets_total,
                    duration: fetch_start.elapsed(),
                    fetched,
                    failures,
                }
            });
        }

        if rx_closed && in_flight.is_empty() && accumulated.is_empty() {
            break;
        }

        tokio::select! {
            maybe_batch = rx.recv(), if !rx_closed => {
                match maybe_batch {
                    Some(batch) => accumulated.extend(batch),
                    None => rx_closed = true,
                }
            }
            maybe_result = in_flight.next(), if !in_flight.is_empty() => {
                let Some(result) = maybe_result else {
                    continue;
                };
                in_flight_keys_by_batch.remove(&result.batch_id);
                let continue_running = handle_fetch_batch_output(
                    &blockchain_id,
                    result,
                    &tx,
                    &outcome_tx,
                    &mut accumulated,
                    &in_flight_keys_by_batch,
                )
                .await;
                if !continue_running {
                    return;
                }
            }
        }
    }
}

struct FetchBatchOutput {
    batch_id: u64,
    batch_size: usize,
    assets_total: u64,
    duration: Duration,
    fetched: Vec<FetchedKc>,
    failures: Vec<QueueOutcome>,
}

fn take_next_fetch_batch(
    accumulated: &mut Vec<KcToSync>,
    fetch_max_kc_per_batch: usize,
    fetch_max_ka_per_batch: u64,
    allow_partial_batch: bool,
) -> Option<(Vec<KcToSync>, u64)> {
    if accumulated.is_empty()
        || (!allow_partial_batch && accumulated.len() < fetch_max_kc_per_batch)
    {
        return None;
    }

    let mut assets_total = 0u64;
    let mut take = 0usize;
    for kc in accumulated.iter() {
        if take >= UAL_MAX_LIMIT {
            break;
        }
        let kc_assets = estimate_asset_count(&kc.token_ids);
        if take > 0 && assets_total.saturating_add(kc_assets) > fetch_max_ka_per_batch {
            break;
        }
        assets_total = assets_total.saturating_add(kc_assets);
        take += 1;
    }

    if take == 0 {
        take = 1;
        assets_total = estimate_asset_count(&accumulated[0].token_ids);
    }

    Some((accumulated.drain(..take).collect(), assets_total))
}

async fn handle_fetch_batch_output(
    blockchain_id: &BlockchainId,
    result: FetchBatchOutput,
    tx: &mpsc::Sender<Vec<FetchedKc>>,
    outcome_tx: &mpsc::Sender<Vec<QueueOutcome>>,
    accumulated: &mut Vec<KcToSync>,
    in_flight_keys_by_batch: &HashMap<u64, Vec<QueueKcKey>>,
) -> bool {
    let batch_status = if result.failures.is_empty() {
        "success"
    } else if result.fetched.is_empty() {
        "failed"
    } else {
        "partial"
    };
    observability::record_sync_fetch_batch(
        batch_status,
        result.duration,
        result.batch_size,
        result.assets_total,
    );

    observability::record_sync_kc_outcome(
        blockchain_id.as_str(),
        "fetch",
        "failed",
        result.failures.len(),
    );
    let failure_count = result.failures.len();
    if failure_count > 0 && outcome_tx.send(result.failures).await.is_err() {
        tracing::warn!(
            blockchain_id = %blockchain_id,
            failure_count,
            "Fetch: queue outcome receiver dropped while sending failed outcomes"
        );
        return false;
    }

    if result.fetched.is_empty() {
        return true;
    }

    tracing::trace!(
        batch_size = result.batch_size,
        assets_in_batch = result.assets_total,
        fetched_count = result.fetched.len(),
        failed_count = failure_count,
        fetch_ms = result.duration.as_millis() as u64,
        "Fetch: sending batch to insert stage"
    );

    match tx.send(result.fetched).await {
        Ok(()) => true,
        Err(send_error) => {
            let unsent_fetched = send_error.0;
            tracing::warn!(
                blockchain_id = %blockchain_id,
                unsent_fetched = unsent_fetched.len(),
                remaining_accumulated = accumulated.len(),
                in_flight_batches = in_flight_keys_by_batch.len(),
                "Fetch: insert stage receiver dropped, requeueing pending KCs"
            );
            let retry_outcomes = collect_pending_retry_outcomes(
                unsent_fetched,
                accumulated,
                in_flight_keys_by_batch,
            );
            if !retry_outcomes.is_empty() {
                let _ = outcome_tx.send(retry_outcomes).await;
            }
            false
        }
    }
}

fn collect_pending_retry_outcomes(
    unsent_fetched: Vec<FetchedKc>,
    accumulated: &mut Vec<KcToSync>,
    in_flight_keys_by_batch: &HashMap<u64, Vec<QueueKcKey>>,
) -> Vec<QueueOutcome> {
    let mut seen = HashSet::new();
    let mut outcomes = Vec::new();

    for key in unsent_fetched
        .into_iter()
        .map(|kc| kc.key)
        .chain(accumulated.drain(..).map(|kc| kc.key))
        .chain(
            in_flight_keys_by_batch
                .values()
                .flat_map(|keys| keys.iter().cloned()),
        )
    {
        if seen.insert(key.clone()) {
            outcomes.push(QueueOutcome::retry_with_pending_error(
                key,
                FETCH_STAGE_FAILURE_REASON,
            ));
        }
    }

    outcomes
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
    max_peer_attempts_per_batch: Option<usize>,
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
            .map(|kc| {
                QueueOutcome::retry_with_pending_error(kc.key.clone(), FETCH_STAGE_FAILURE_REASON)
            })
            .collect();
        return (Vec::new(), failures);
    }

    peer_registry.sort_by_latency(&mut peers);
    let available_peers = peers.len();
    if let Some(max_peer_attempts_per_batch) = max_peer_attempts_per_batch
        && available_peers > max_peer_attempts_per_batch
    {
        peers.truncate(max_peer_attempts_per_batch);
        tracing::trace!(
            blockchain_id = %blockchain_id,
            batch_kcs = kcs.len(),
            available_peers,
            max_peer_attempts_per_batch,
            "Fetch: limiting peer attempts for batch"
        );
    }
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
    let concurrent_peers = batch_get_fanout_concurrency.max(1);

    // Build lookup maps
    let ual_to_kc: HashMap<&str, &KcToSync> = kcs.iter().map(|kc| (kc.ual.as_str(), kc)).collect();

    // Parse UALs for validation
    let parsed_uals: HashMap<&str, ParsedUal> = kcs
        .iter()
        .filter_map(|kc| {
            parse_ual(&kc.ual)
                .ok()
                .map(|parsed| (kc.ual.as_str(), parsed))
        })
        .collect();

    // Build a shared token IDs map once and reuse it for peer requests.
    let token_ids_map = Arc::new(
        kcs.iter()
            .map(|kc| (kc.ual.clone(), kc.token_ids.clone()))
            .collect::<HashMap<String, TokenIds>>(),
    );

    // Helper to build batch request for remaining UALs
    let build_request = |uals: &HashSet<String>| {
        BatchGetRequestData::new_with_shared_token_ids(
            blockchain_id.to_string(),
            uals.iter().cloned().collect(),
            Arc::clone(&token_ids_map),
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
                        (parsed_uals.get(ual.as_str()), ual_to_kc.get(ual.as_str()))
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
                                key: kc.key.clone(),
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
            ual_to_kc.get(ual.as_str()).map(|kc| {
                QueueOutcome::retry_with_pending_error(kc.key.clone(), FETCH_STAGE_FAILURE_REASON)
            })
        })
        .collect();

    tracing::Span::current().record("fetched", fetched.len());
    tracing::Span::current().record("failed", failed.len());

    (fetched, failed)
}
