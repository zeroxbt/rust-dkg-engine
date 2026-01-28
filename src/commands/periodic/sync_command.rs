use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{StreamExt, future::join_all, stream::FuturesUnordered};
use tokio::sync::mpsc;

use crate::{
    commands::{
        command_executor::CommandExecutionResult, command_registry::CommandHandler,
        operations::get::protocols::batch_get::BATCH_GET_UAL_MAX_LIMIT,
    },
    context::Context,
    managers::{
        blockchain::{Address, BlockchainId, BlockchainManager, ContractName},
        network::{NetworkManager, PeerId, messages::BatchGetRequestData},
        repository::RepositoryManager,
        triple_store::{
            Assertion, KnowledgeCollectionMetadata, MAX_TOKENS_PER_KC, TokenIds, Visibility,
            query::predicates,
            rdf::{
                extract_datetime_as_unix, extract_quoted_integer, extract_quoted_string,
                extract_uri_suffix,
            },
        },
    },
    services::{GetValidationService, TripleStoreService},
    utils::ual::{ParsedUal, derive_ual, parse_ual},
};

/// Interval between sync cycles (30 seconds)
const SYNC_PERIOD: Duration = Duration::from_secs(0);

const MAX_NEW_KCS_PER_CONTRACT: u64 = BATCH_GET_UAL_MAX_LIMIT as u64 / MAX_RETRY_ATTEMPTS as u64;

/// Maximum retry attempts before a KC is no longer retried (stays in DB for future recovery)
const MAX_RETRY_ATTEMPTS: u32 = 2;

/// Batch size for filter task (KCs per batch sent through channel)
const FILTER_BATCH_SIZE: usize = 50;

/// Batch size for network fetch (start fetching when we have this many KCs).
/// Set to match FILTER_BATCH_SIZE to start fetching as soon as first filter batch completes.
/// This enables true pipeline overlap: fetch starts while filter is still processing.
const NETWORK_FETCH_BATCH_SIZE: usize = 50;

/// Channel buffer size (number of batches that can be buffered between stages)
const PIPELINE_CHANNEL_BUFFER: usize = 3;

/// Number of peers to query concurrently during network fetch
const CONCURRENT_PEER_REQUESTS: usize = 3;

pub(crate) struct SyncCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
    triple_store_service: Arc<TripleStoreService>,
    network_manager: Arc<NetworkManager>,
    get_validation_service: Arc<GetValidationService>,
}

/// Result of syncing a single contract
struct ContractSyncResult {
    enqueued: u64,
    pending: usize,
    synced: u64,
    failed: u64,
}

/// KC that needs to be fetched from the network (output of filter stage)
#[derive(Clone)]
struct KcToSync {
    kc_id: u64,
    ual: String,
    start_token_id: u64,
    end_token_id: u64,
    burned: Vec<u64>,
}

/// KC fetched from network (output of fetch stage, input to insert stage)
#[derive(Clone)]
struct FetchedKc {
    kc_id: u64,
    ual: String,
    assertion: Assertion,
    metadata: Option<KnowledgeCollectionMetadata>,
}

/// Stats collected by filter task
struct FilterStats {
    already_synced: Vec<u64>,
}

/// Stats collected by fetch task
struct FetchStats {
    failures: Vec<u64>,
}

/// Stats collected by insert task
struct InsertStats {
    synced: Vec<u64>,
    failed: Vec<u64>,
    expired: Vec<u64>,
}

// ============================================================================
// Pipeline Task Functions
// ============================================================================

/// Filter task: processes pending KCs in batches, checking RPC for token ranges
/// and triple store for local existence. Sends KCs that need syncing to fetch stage.
async fn filter_task(
    pending_kc_ids: Vec<u64>,
    blockchain_id: BlockchainId,
    contract_address: Address,
    contract_addr_str: String,
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_service: Arc<TripleStoreService>,
    tx: mpsc::Sender<Vec<KcToSync>>,
) -> FilterStats {
    let task_start = Instant::now();
    let mut already_synced = Vec::new();
    let total_kcs = pending_kc_ids.len();
    let mut processed = 0usize;

    for chunk in pending_kc_ids.chunks(FILTER_BATCH_SIZE) {
        // Step 1: Fetch token ranges in parallel (rate limiter will throttle)
        let range_futures: Vec<_> = chunk
            .iter()
            .map(|&kc_id| {
                let bm = Arc::clone(&blockchain_manager);
                let bid = blockchain_id.clone();
                async move {
                    let result = bm
                        .get_knowledge_assets_range(&bid, contract_address, kc_id as u128)
                        .await;
                    (kc_id, result)
                }
            })
            .collect();

        let range_results = join_all(range_futures).await;

        // Process RPC results - collect KCs with valid token ranges
        struct KcWithRange {
            kc_id: u64,
            ual: String,
            start_token_id: u64,
            end_token_id: u64,
            burned: Vec<u64>,
        }

        let mut kcs_with_ranges: Vec<KcWithRange> = Vec::new();

        for (kc_id, result) in range_results {
            match result {
                Ok(Some((global_start, global_end, global_burned))) => {
                    let offset = (kc_id - 1) * MAX_TOKENS_PER_KC;
                    let start_token_id = global_start.saturating_sub(offset);
                    let end_token_id = global_end.saturating_sub(offset);
                    let burned: Vec<u64> = global_burned
                        .into_iter()
                        .map(|b| b.saturating_sub(offset))
                        .collect();

                    let kc_ual = derive_ual(&blockchain_id, &contract_address, kc_id as u128, None);

                    kcs_with_ranges.push(KcWithRange {
                        kc_id,
                        ual: kc_ual,
                        start_token_id,
                        end_token_id,
                        burned,
                    });
                }
                Ok(None) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc_id,
                        "[DKG SYNC] KC has no token range on chain, skipping"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc_id,
                        error = %e,
                        "[DKG SYNC] Failed to get KC token range, will retry later"
                    );
                }
            }
        }

        // Step 2: Check local existence in parallel (semaphore will limit concurrency)
        let existence_futures: Vec<_> = kcs_with_ranges
            .iter()
            .map(|kc| {
                let ts = Arc::clone(&triple_store_service);
                let ual = kc.ual.clone();
                let start = kc.start_token_id;
                let end = kc.end_token_id;
                async move {
                    let exists = ts.knowledge_collection_exists(&ual, start, end).await;
                    exists
                }
            })
            .collect();

        let existence_results = join_all(existence_futures).await;

        // Step 3: Partition into to_sync and already_synced
        let mut batch_to_sync = Vec::new();
        for (kc, exists_locally) in kcs_with_ranges.into_iter().zip(existence_results) {
            if exists_locally {
                already_synced.push(kc.kc_id);
            } else {
                batch_to_sync.push(KcToSync {
                    kc_id: kc.kc_id,
                    ual: kc.ual,
                    start_token_id: kc.start_token_id,
                    end_token_id: kc.end_token_id,
                    burned: kc.burned,
                });
            }
        }

        // Send to fetch stage (blocks if channel full - backpressure)
        processed += chunk.len();
        if !batch_to_sync.is_empty() {
            tracing::info!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                batch_size = batch_to_sync.len(),
                processed,
                total_kcs,
                elapsed_ms = task_start.elapsed().as_millis() as u64,
                "[DKG SYNC] Filter: sending batch to fetch stage"
            );
            if tx.send(batch_to_sync).await.is_err() {
                tracing::debug!("[DKG SYNC] Filter: fetch stage receiver dropped, stopping");
                break;
            }
        }
    }

    tracing::info!(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
        total_ms = task_start.elapsed().as_millis() as u64,
        already_synced = already_synced.len(),
        "[DKG SYNC] Filter task completed"
    );

    FilterStats { already_synced }
}

/// Fetch task: receives filtered KCs, fetches from network, sends to insert stage.
async fn fetch_task(
    mut rx: mpsc::Receiver<Vec<KcToSync>>,
    blockchain_id: BlockchainId,
    network_manager: Arc<NetworkManager>,
    repository_manager: Arc<RepositoryManager>,
    get_validation_service: Arc<GetValidationService>,
    tx: mpsc::Sender<Vec<FetchedKc>>,
) -> FetchStats {
    let task_start = Instant::now();
    let mut failures: Vec<u64> = Vec::new();
    let mut total_fetched = 0usize;
    let mut total_received = 0usize;

    // Get shard peers once at the start
    let peers = match get_shard_peers(&blockchain_id, &network_manager, &repository_manager).await {
        Ok(peers) => peers,
        Err(e) => {
            tracing::error!(
                blockchain_id = %blockchain_id,
                error = %e,
                "[DKG SYNC] Fetch: failed to get shard peers"
            );
            // Drain receiver and mark all as failed
            while let Some(batch) = rx.recv().await {
                failures.extend(batch.iter().map(|kc| kc.kc_id));
            }
            return FetchStats { failures };
        }
    };

    if peers.is_empty() {
        tracing::warn!(
            blockchain_id = %blockchain_id,
            "[DKG SYNC] Fetch: no peers available"
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
        // Using NETWORK_FETCH_BATCH_SIZE (50) instead of BATCH_GET_UAL_MAX_LIMIT (500)
        // allows the pipeline to overlap: fetch starts while filter is still processing
        while accumulated.len() >= NETWORK_FETCH_BATCH_SIZE
            || (!accumulated.is_empty() && rx.is_empty())
        {
            let batch_size = accumulated.len().min(BATCH_GET_UAL_MAX_LIMIT);
            let to_fetch: Vec<KcToSync> = accumulated.drain(..batch_size).collect();

            let fetch_start = Instant::now();
            let (fetched, batch_failures) = fetch_kc_batch_from_network(
                &blockchain_id,
                &to_fetch,
                &peers,
                &network_manager,
                &get_validation_service,
            )
            .await;

            total_fetched += fetched.len();

            // Failed KCs have already been tried with all peers - mark as final failures
            failures.extend(batch_failures);

            // Send fetched KCs to insert stage
            if !fetched.is_empty() {
                tracing::info!(
                    blockchain_id = %blockchain_id,
                    batch_size = to_fetch.len(),
                    fetched_count = fetched.len(),
                    failed_count = failures.len(),
                    fetch_ms = fetch_start.elapsed().as_millis() as u64,
                    total_received,
                    total_fetched,
                    elapsed_ms = task_start.elapsed().as_millis() as u64,
                    "[DKG SYNC] Fetch: sending batch to insert stage"
                );
                if tx.send(fetched).await.is_err() {
                    tracing::debug!("[DKG SYNC] Fetch: insert stage receiver dropped, stopping");
                    // Mark remaining accumulated as failed
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
        let (fetched, batch_failures) = fetch_kc_batch_from_network(
            &blockchain_id,
            &accumulated,
            &peers,
            &network_manager,
            get_validation_service.as_ref(),
        )
        .await;

        total_fetched += fetched.len();
        failures.extend(batch_failures);

        if !fetched.is_empty() {
            tracing::info!(
                blockchain_id = %blockchain_id,
                remaining = accumulated.len(),
                fetched_count = fetched.len(),
                final_failures = failures.len(),
                fetch_ms = fetch_start.elapsed().as_millis() as u64,
                "[DKG SYNC] Fetch: flushing remaining KCs"
            );
            let _ = tx.send(fetched).await;
        }
    }

    tracing::info!(
        blockchain_id = %blockchain_id,
        total_ms = task_start.elapsed().as_millis() as u64,
        total_fetched,
        failures = failures.len(),
        "[DKG SYNC] Fetch task completed"
    );

    FetchStats { failures }
}

/// Insert task: receives fetched KCs, checks expiration, inserts into triple store.
async fn insert_task(
    mut rx: mpsc::Receiver<Vec<FetchedKc>>,
    blockchain_id: BlockchainId,
    contract_address: Address,
    contract_addr_str: String,
    blockchain_manager: Arc<BlockchainManager>,
    triple_store_service: Arc<TripleStoreService>,
) -> InsertStats {
    let task_start = Instant::now();
    let mut synced = Vec::new();
    let mut failed = Vec::new();
    let mut expired = Vec::new();
    let mut total_received = 0usize;

    // Get current epoch once for expiration checks
    let current_epoch = blockchain_manager
        .get_current_epoch(&blockchain_id)
        .await
        .ok();

    while let Some(batch) = rx.recv().await {
        let batch_start = Instant::now();
        total_received += batch.len();
        tracing::info!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            batch_size = batch.len(),
            total_received,
            elapsed_ms = task_start.elapsed().as_millis() as u64,
            "[DKG SYNC] Insert: received batch"
        );

        // Step 1: Fetch all end epochs in parallel (rate limiter will throttle)
        let epoch_futures: Vec<_> = batch
            .iter()
            .map(|kc| {
                let bm = Arc::clone(&blockchain_manager);
                let bid = blockchain_id.clone();
                let kc_id = kc.kc_id;
                async move {
                    let result = bm
                        .get_kc_end_epoch(&bid, contract_address, kc_id as u128)
                        .await;
                    (kc_id, result)
                }
            })
            .collect();

        let epoch_results = join_all(epoch_futures).await;
        let epoch_map: HashMap<u64, Result<u64, _>> = epoch_results.into_iter().collect();

        // Step 2: Filter by expiration
        let mut valid_kcs = Vec::new();
        for kc in batch {
            if let Some(current) = current_epoch {
                match epoch_map.get(&kc.kc_id) {
                    Some(Ok(end_epoch)) if current > *end_epoch => {
                        tracing::debug!(
                            blockchain_id = %blockchain_id,
                            contract = %contract_addr_str,
                            kc_id = kc.kc_id,
                            current_epoch = current,
                            end_epoch,
                            "[DKG SYNC] KC is expired, skipping insert"
                        );
                        expired.push(kc.kc_id);
                        continue;
                    }
                    Some(Err(e)) => {
                        tracing::warn!(
                            blockchain_id = %blockchain_id,
                            contract = %contract_addr_str,
                            kc_id = kc.kc_id,
                            error = %e,
                            "[DKG SYNC] Failed to get KC end epoch, will retry later"
                        );
                        failed.push(kc.kc_id);
                        continue;
                    }
                    None => {
                        tracing::warn!(
                            blockchain_id = %blockchain_id,
                            contract = %contract_addr_str,
                            kc_id = kc.kc_id,
                            "[DKG SYNC] Missing epoch result, will retry later"
                        );
                        failed.push(kc.kc_id);
                        continue;
                    }
                    _ => {}
                }
            }
            valid_kcs.push(kc);
        }

        // Step 3: Insert all valid KCs in parallel (semaphore will limit concurrency)
        let insert_futures: Vec<_> = valid_kcs
            .iter()
            .map(|kc| {
                let ts = Arc::clone(&triple_store_service);
                let ual = kc.ual.clone();
                let assertion = kc.assertion.clone();
                let metadata = kc.metadata.clone();
                let kc_id = kc.kc_id;
                async move {
                    let result = ts
                        .insert_knowledge_collection(&ual, &assertion, &metadata)
                        .await;
                    (kc_id, ual, result)
                }
            })
            .collect();

        let insert_results = join_all(insert_futures).await;

        for (kc_id, ual, result) in insert_results {
            match result {
                Ok(triples_inserted) => {
                    tracing::debug!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc_id,
                        ual = %ual,
                        triples = triples_inserted,
                        "[DKG SYNC] Stored synced KC in triple store"
                    );
                    synced.push(kc_id);
                }
                Err(e) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc_id,
                        error = %e,
                        "[DKG SYNC] Failed to store KC in triple store"
                    );
                    failed.push(kc_id);
                }
            }
        }

        tracing::info!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            batch_ms = batch_start.elapsed().as_millis() as u64,
            batch_synced = synced.len(),
            total_synced = synced.len(),
            "[DKG SYNC] Insert: batch completed"
        );
    }

    tracing::info!(
        blockchain_id = %blockchain_id,
        contract = %contract_addr_str,
        total_ms = task_start.elapsed().as_millis() as u64,
        synced = synced.len(),
        failed = failed.len(),
        expired = expired.len(),
        "[DKG SYNC] Insert task completed"
    );

    InsertStats {
        synced,
        failed,
        expired,
    }
}

/// Get shard peers for the given blockchain, excluding self.
async fn get_shard_peers(
    blockchain_id: &BlockchainId,
    network_manager: &NetworkManager,
    repository_manager: &RepositoryManager,
) -> Result<Vec<PeerId>, String> {
    let shard_nodes = repository_manager
        .shard_repository()
        .get_all_peer_records(blockchain_id.as_str())
        .await
        .map_err(|e| format!("Failed to get shard nodes: {}", e))?;

    let my_peer_id = *network_manager.peer_id();
    let peers: Vec<PeerId> = shard_nodes
        .iter()
        .filter_map(|record| record.peer_id.parse().ok())
        .filter(|peer_id| *peer_id != my_peer_id)
        .collect();

    Ok(peers)
}

/// Helper function for making peer requests.
/// Using a named async fn instead of async blocks allows FuturesUnordered
/// to work without boxing, since all calls return the same concrete future type.
async fn make_peer_request(
    peer: PeerId,
    request_data: BatchGetRequestData,
    nm: Arc<NetworkManager>,
) -> (
    PeerId,
    Result<
        crate::managers::network::messages::BatchGetResponseData,
        crate::managers::network::NetworkError,
    >,
) {
    let addresses = nm.get_peer_addresses(peer).await.unwrap_or_default();
    let result = nm
        .send_batch_get_request(peer, addresses, uuid::Uuid::new_v4(), request_data)
        .await;
    (peer, result)
}

/// Fetch a batch of KCs from the network.
/// Returns (fetched KCs, failed KC IDs).
///
/// Uses FuturesUnordered to process peer responses as they arrive,
/// avoiding being blocked by slow/unreachable peers.
async fn fetch_kc_batch_from_network(
    blockchain_id: &BlockchainId,
    kcs: &[KcToSync],
    peers: &[PeerId],
    network_manager: &Arc<NetworkManager>,
    get_validation_service: &GetValidationService,
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
        .map(|kc| {
            let token_ids = TokenIds::new(kc.start_token_id, kc.end_token_id, kc.burned.clone());
            (kc.ual.clone(), token_ids)
        })
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

    // Use FuturesUnordered to process responses as they arrive.
    // This avoids waiting for slow peers - fast peers' responses are processed immediately.
    let mut futures = FuturesUnordered::new();
    let mut peers_iter = peers.iter();

    // Build initial request with all UALs
    let initial_request = build_request(&uals_still_needed);

    // Start initial batch of concurrent requests
    for _ in 0..CONCURRENT_PEER_REQUESTS {
        if let Some(&peer) = peers_iter.next() {
            futures.push(make_peer_request(
                peer,
                initial_request.clone(),
                Arc::clone(network_manager),
            ));
        }
    }

    // Process responses as they complete, adding new peers as slots free up
    while let Some((peer, result)) = futures.next().await {
        // Check if we're done (have all KCs we need)
        if uals_still_needed.is_empty() {
            break;
        }

        match result {
            Ok(response) => {
                let metadata_map = response.metadata();

                for (ual, assertion) in response.assertions() {
                    if !uals_still_needed.contains(ual) {
                        continue;
                    }

                    if !assertion.has_data() {
                        continue;
                    }

                    if let Some(parsed_ual) = parsed_uals.get(ual) {
                        let is_valid = get_validation_service
                            .validate_response(assertion, parsed_ual, Visibility::All)
                            .await;

                        if is_valid {
                            if let Some(kc) = ual_to_kc.get(ual.as_str()) {
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

                                tracing::debug!(
                                    ual = %ual,
                                    peer = %peer,
                                    "[DKG SYNC] Received and validated KC from network"
                                );
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!(
                    peer = %peer,
                    error = %e,
                    "[DKG SYNC] Request to peer failed"
                );
            }
        }

        // Add next peer if we still need KCs and have more peers available
        // Rebuild request with only remaining UALs to avoid unnecessary data transfer
        if !uals_still_needed.is_empty() {
            if let Some(&next_peer) = peers_iter.next() {
                let updated_request = build_request(&uals_still_needed);
                futures.push(make_peer_request(
                    next_peer,
                    updated_request,
                    Arc::clone(network_manager),
                ));
            }
        }
    }

    // Map remaining UALs back to KC IDs for the failed list
    let failed_kc_ids: Vec<u64> = uals_still_needed
        .iter()
        .filter_map(|ual| ual_to_kc.get(ual.as_str()).map(|kc| kc.kc_id))
        .collect();

    (fetched, failed_kc_ids)
}

// ============================================================================
// SyncCommandHandler Implementation
// ============================================================================

impl SyncCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            network_manager: Arc::clone(context.network_manager()),
            get_validation_service: Arc::clone(context.get_validation_service()),
        }
    }

    /// Sync a single contract: enqueue new KCs from chain, then process pending KCs.
    ///
    /// Uses a three-stage pipeline with spawned tasks connected by channels:
    /// 1. Filter task: checks RPC for token ranges, triple store for local existence
    /// 2. Fetch task: fetches KCs from network peers
    /// 3. Insert task: checks expiration, inserts into triple store
    ///
    /// This allows phases to overlap, reducing total sync time.
    #[tracing::instrument(
        skip(self),
        fields(
            blockchain_id = %blockchain_id,
            contract = %format!("{:?}", contract_address),
        )
    )]
    async fn sync_contract(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
    ) -> Result<ContractSyncResult, String> {
        let contract_addr_str = format!("{:?}", contract_address);
        let sync_start = Instant::now();

        // Step 1: Check for new KCs on chain and enqueue them
        let enqueue_start = Instant::now();
        let enqueued = self
            .enqueue_new_kcs(blockchain_id, contract_address, &contract_addr_str)
            .await?;
        let enqueue_ms = enqueue_start.elapsed().as_millis();

        // Step 2: Fetch pending KCs for this contract from DB (limited to avoid long cycles)
        let db_start = Instant::now();
        let pending_kcs = self
            .repository_manager
            .kc_sync_repository()
            .get_pending_kcs_for_contract(
                blockchain_id.as_str(),
                &contract_addr_str,
                MAX_RETRY_ATTEMPTS,
                MAX_NEW_KCS_PER_CONTRACT,
            )
            .await
            .map_err(|e| format!("Failed to fetch pending KCs: {}", e))?;
        let db_fetch_ms = db_start.elapsed().as_millis();

        let pending = pending_kcs.len();

        if pending == 0 {
            tracing::debug!(enqueue_ms, db_fetch_ms, "[DKG SYNC] No pending KCs");
            return Ok(ContractSyncResult {
                enqueued,
                pending: 0,
                synced: 0,
                failed: 0,
            });
        }

        let pending_kc_ids: Vec<u64> = pending_kcs.into_iter().map(|kc| kc.kc_id).collect();

        // Step 3: Create pipeline channels
        let (filter_tx, filter_rx) = mpsc::channel::<Vec<KcToSync>>(PIPELINE_CHANNEL_BUFFER);
        let (fetch_tx, fetch_rx) = mpsc::channel::<Vec<FetchedKc>>(PIPELINE_CHANNEL_BUFFER);

        // Step 4: Spawn pipeline tasks
        let pipeline_start = Instant::now();

        let filter_handle = {
            let blockchain_id = blockchain_id.clone();
            let contract_addr_str = contract_addr_str.clone();
            let blockchain_manager = Arc::clone(&self.blockchain_manager);
            let triple_store_service = Arc::clone(&self.triple_store_service);
            tokio::spawn(async move {
                filter_task(
                    pending_kc_ids,
                    blockchain_id,
                    contract_address,
                    contract_addr_str,
                    blockchain_manager,
                    triple_store_service,
                    filter_tx,
                )
                .await
            })
        };

        let fetch_handle = {
            let blockchain_id = blockchain_id.clone();
            let network_manager = Arc::clone(&self.network_manager);
            let repository_manager = Arc::clone(&self.repository_manager);
            let get_validation_service = Arc::clone(&self.get_validation_service);
            tokio::spawn(async move {
                fetch_task(
                    filter_rx,
                    blockchain_id,
                    network_manager,
                    repository_manager,
                    get_validation_service,
                    fetch_tx,
                )
                .await
            })
        };

        let insert_handle = {
            let blockchain_id = blockchain_id.clone();
            let contract_addr_str = contract_addr_str.clone();
            let blockchain_manager = Arc::clone(&self.blockchain_manager);
            let triple_store_service = Arc::clone(&self.triple_store_service);
            tokio::spawn(async move {
                insert_task(
                    fetch_rx,
                    blockchain_id,
                    contract_address,
                    contract_addr_str,
                    blockchain_manager,
                    triple_store_service,
                )
                .await
            })
        };

        // Step 5: Wait for all tasks to complete
        let (filter_result, fetch_result, insert_result) =
            tokio::join!(filter_handle, fetch_handle, insert_handle);

        let pipeline_ms = pipeline_start.elapsed().as_millis();

        // Step 6: Collect results from tasks
        let filter_stats = filter_result.map_err(|e| format!("Filter task panicked: {}", e))?;
        let fetch_stats = fetch_result.map_err(|e| format!("Fetch task panicked: {}", e))?;
        let insert_stats = insert_result.map_err(|e| format!("Insert task panicked: {}", e))?;

        // Step 7: Update DB with results
        // Remove already-synced KCs (found locally in filter stage)
        if !filter_stats.already_synced.is_empty() {
            if let Err(e) = self
                .repository_manager
                .kc_sync_repository()
                .remove_kcs(
                    blockchain_id.as_str(),
                    &contract_addr_str,
                    &filter_stats.already_synced,
                )
                .await
            {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    error = %e,
                    "[DKG SYNC] Failed to remove already-synced KCs from queue"
                );
            }
        }

        // Remove expired KCs (they should not be retried)
        if !insert_stats.expired.is_empty() {
            if let Err(e) = self
                .repository_manager
                .kc_sync_repository()
                .remove_kcs(
                    blockchain_id.as_str(),
                    &contract_addr_str,
                    &insert_stats.expired,
                )
                .await
            {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    error = %e,
                    "[DKG SYNC] Failed to remove expired KCs from queue"
                );
            }
        }

        // Remove successfully synced KCs
        if !insert_stats.synced.is_empty() {
            if let Err(e) = self
                .repository_manager
                .kc_sync_repository()
                .remove_kcs(
                    blockchain_id.as_str(),
                    &contract_addr_str,
                    &insert_stats.synced,
                )
                .await
            {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    error = %e,
                    "[DKG SYNC] Failed to remove synced KCs from queue"
                );
            }
        }

        // Capture counts before moving vectors
        let fetch_failures_count = fetch_stats.failures.len();
        let insert_failures_count = insert_stats.failed.len();

        // Increment retry count for failed KCs (from both fetch and insert stages)
        let mut all_failed: Vec<u64> = fetch_stats.failures;
        all_failed.extend(insert_stats.failed);

        if !all_failed.is_empty() {
            if let Err(e) = self
                .repository_manager
                .kc_sync_repository()
                .increment_retry_count(blockchain_id.as_str(), &contract_addr_str, &all_failed)
                .await
            {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    error = %e,
                    "[DKG SYNC] Failed to increment retry count for failed KCs"
                );
            }
        }

        // Calculate totals
        let synced = filter_stats.already_synced.len() as u64 + insert_stats.synced.len() as u64;
        let failed = all_failed.len() as u64;

        let total_ms = sync_start.elapsed().as_millis();

        tracing::info!(
            total_ms,
            enqueue_ms,
            db_fetch_ms,
            pipeline_ms,
            kcs_pending = pending,
            kcs_already_synced = filter_stats.already_synced.len(),
            kcs_fetch_failed = fetch_failures_count,
            kcs_synced = insert_stats.synced.len(),
            kcs_insert_failed = insert_failures_count,
            kcs_expired = insert_stats.expired.len(),
            "[DKG SYNC] Contract sync timing breakdown (pipelined)"
        );

        Ok(ContractSyncResult {
            enqueued,
            pending,
            synced,
            failed,
        })
    }

    /// Check for new KCs on chain and enqueue any that need syncing.
    async fn enqueue_new_kcs(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        contract_addr_str: &str,
    ) -> Result<u64, String> {
        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            "[DKG SYNC] Fetching latest KC ID from chain..."
        );

        // Get latest KC ID from chain
        let latest_on_chain = self
            .blockchain_manager
            .get_latest_knowledge_collection_id(blockchain_id, contract_address)
            .await
            .map_err(|e| format!("Failed to get latest KC ID: {}", e))?;

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            latest_on_chain,
            "[DKG SYNC] Got latest KC ID from chain"
        );

        // Get our last checked ID from DB
        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            "[DKG SYNC] Fetching sync progress from DB..."
        );

        let last_checked = self
            .repository_manager
            .kc_sync_repository()
            .get_progress(blockchain_id.as_str(), contract_addr_str)
            .await
            .map_err(|e| format!("Failed to get sync progress: {}", e))?
            .map(|p| p.last_checked_id)
            .unwrap_or(0);

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            last_checked,
            "[DKG SYNC] Got sync progress from DB"
        );

        // Nothing new to check
        if latest_on_chain <= last_checked {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                "[DKG SYNC] No new KCs to enqueue (latest_on_chain <= last_checked)"
            );
            return Ok(0);
        }

        // Calculate range of new KC IDs to enqueue (limited to avoid huge batches)
        let start_id = last_checked + 1;
        let end_id = std::cmp::min(latest_on_chain, last_checked + MAX_NEW_KCS_PER_CONTRACT);
        let new_kc_ids: Vec<u64> = (start_id..=end_id).collect();
        let count = new_kc_ids.len() as u64;

        // Enqueue all new KCs (expiration is checked later, after fetching from network)
        self.repository_manager
            .kc_sync_repository()
            .enqueue_kcs(blockchain_id.as_str(), contract_addr_str, &new_kc_ids)
            .await
            .map_err(|e| format!("Failed to enqueue KCs: {}", e))?;

        // Update progress to the highest ID we've now checked
        self.repository_manager
            .kc_sync_repository()
            .upsert_progress(blockchain_id.as_str(), contract_addr_str, end_id)
            .await
            .map_err(|e| format!("Failed to update progress: {}", e))?;

        Ok(count)
    }
}

/// Parse metadata from network RDF triples.
///
/// Returns KnowledgeCollectionMetadata if all required fields are found, None otherwise.
fn parse_metadata_from_triples(triples: &[String]) -> Option<KnowledgeCollectionMetadata> {
    let mut publisher_address: Option<String> = None;
    let mut block_number: Option<u64> = None;
    let mut transaction_hash: Option<String> = None;
    let mut block_timestamp: Option<u64> = None;

    for triple in triples {
        if triple.contains(predicates::PUBLISHED_BY) {
            // Format: <ual> <predicate> <did:dkg:publisherKey/0x123...> .
            publisher_address = extract_uri_suffix(triple, predicates::PUBLISHER_KEY_PREFIX);
        } else if triple.contains(predicates::PUBLISHED_AT_BLOCK) {
            // Format: <ual> <predicate> "12345" .
            block_number = extract_quoted_integer(triple);
        } else if triple.contains(predicates::PUBLISH_TX) {
            // Format: <ual> <predicate> "0x..." .
            transaction_hash = extract_quoted_string(triple);
        } else if triple.contains(predicates::BLOCK_TIME) {
            // Format: <ual> <predicate> "2024-01-15T10:30:00Z"^^<xsd:dateTime> .
            block_timestamp = extract_datetime_as_unix(triple);
        }
    }

    // All fields are required
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

#[derive(Clone)]
pub(crate) struct SyncCommandData {
    pub blockchain_id: BlockchainId,
}

impl SyncCommandData {
    pub(crate) fn new(blockchain_id: BlockchainId) -> Self {
        Self { blockchain_id }
    }
}

impl CommandHandler<SyncCommandData> for SyncCommandHandler {
    async fn execute(&self, data: &SyncCommandData) -> CommandExecutionResult {
        tracing::info!(
            blockchain_id = %data.blockchain_id,
            "[DKG SYNC] Starting sync cycle"
        );

        // Get all KC storage contract addresses for this blockchain
        let contract_addresses = match self
            .blockchain_manager
            .get_all_contract_addresses(
                &data.blockchain_id,
                &ContractName::KnowledgeCollectionStorage,
            )
            .await
        {
            Ok(addresses) => addresses,
            Err(e) => {
                tracing::error!(
                    blockchain_id = %data.blockchain_id,
                    error = %e,
                    "[DKG SYNC] Failed to get KC storage contract addresses"
                );
                return CommandExecutionResult::Repeat { delay: SYNC_PERIOD };
            }
        };

        tracing::debug!(
            blockchain_id = %data.blockchain_id,
            contract_count = contract_addresses.len(),
            "[DKG SYNC] Found KC storage contracts"
        );

        // Sync each contract in parallel
        let sync_futures = contract_addresses
            .iter()
            .map(|&contract_address| self.sync_contract(&data.blockchain_id, contract_address));

        let results = join_all(sync_futures).await;

        // Aggregate results
        let mut total_enqueued = 0u64;
        let mut total_pending = 0usize;
        let mut total_synced = 0u64;
        let mut total_failed = 0u64;

        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(r) => {
                    total_enqueued += r.enqueued;
                    total_pending += r.pending;
                    total_synced += r.synced;
                    total_failed += r.failed;

                    if r.enqueued > 0 || r.pending > 0 {
                        tracing::debug!(
                            blockchain_id = %data.blockchain_id,
                            contract = ?contract_addresses[i],
                            enqueued = r.enqueued,
                            pending = r.pending,
                            synced = r.synced,
                            failed = r.failed,
                            "[DKG SYNC] Contract sync completed"
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        blockchain_id = %data.blockchain_id,
                        contract = ?contract_addresses[i],
                        error = %e,
                        "[DKG SYNC] Failed to sync contract"
                    );
                }
            }
        }

        if total_enqueued > 0 || total_pending > 0 {
            tracing::info!(
                blockchain_id = %data.blockchain_id,
                total_enqueued,
                total_pending,
                total_synced,
                total_failed,
                "[DKG SYNC] Sync cycle summary"
            );
        }

        tracing::info!(
            blockchain_id = %data.blockchain_id,
            "[DKG SYNC] Sync cycle completed"
        );

        CommandExecutionResult::Repeat { delay: SYNC_PERIOD }
    }
}
