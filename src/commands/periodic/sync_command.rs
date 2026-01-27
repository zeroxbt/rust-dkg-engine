use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::future::join_all;
use metrics::{counter, gauge, histogram};

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
    operations::BatchGetOperation,
    services::{GetValidationService, TripleStoreService, operation::Operation},
    utils::ual::{ParsedUal, derive_ual, parse_ual},
};

/// Interval between sync cycles (30 seconds)
const SYNC_PERIOD: Duration = Duration::from_secs(0);

const MAX_NEW_KCS_PER_CONTRACT: u64 = BATCH_GET_UAL_MAX_LIMIT as u64 / MAX_RETRY_ATTEMPTS as u64;

/// Maximum retry attempts before a KC is no longer retried (stays in DB for future recovery)
const MAX_RETRY_ATTEMPTS: u32 = 2;

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

/// KC that needs to be fetched from the network
#[allow(dead_code)]
struct KcToSync {
    kc_id: u64,
    ual: String,
    start_token_id: u64,
    end_token_id: u64,
    burned: Vec<u64>,
}

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
    async fn sync_contract(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
    ) -> Result<ContractSyncResult, String> {
        let contract_addr_str = format!("{:?}", contract_address);

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            "[DKG SYNC] Starting contract sync"
        );

        // Step 1: Check for new KCs on chain and enqueue them
        let enqueued = self
            .enqueue_new_kcs(blockchain_id, contract_address, &contract_addr_str)
            .await?;

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            enqueued,
            "[DKG SYNC] Enqueue step completed, fetching pending KCs from DB..."
        );

        // Step 2: Fetch pending KCs for this contract from DB (limited to avoid long cycles)
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

        let pending = pending_kcs.len();

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            count = pending,
            "[DKG SYNC] Fetched pending KCs from DB"
        );

        if pending == 0 {
            return Ok(ContractSyncResult {
                enqueued,
                pending: 0,
                synced: 0,
                failed: 0,
            });
        }

        // Step 3: Filter out already-synced KCs (expiration is checked after network fetch)
        let pending_kc_ids: Vec<u64> = pending_kcs.into_iter().map(|kc| kc.kc_id).collect();
        let (kcs_to_sync, already_synced_kc_ids) = self
            .filter_pending_kcs(
                blockchain_id,
                contract_address,
                &contract_addr_str,
                &pending_kc_ids,
            )
            .await;

        // Remove already-synced KCs from queue
        if !already_synced_kc_ids.is_empty() {
            tracing::debug!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                count = already_synced_kc_ids.len(),
                "[DKG SYNC] Removing already-synced KCs from queue"
            );
            self.repository_manager
                .kc_sync_repository()
                .remove_kcs(
                    blockchain_id.as_str(),
                    &contract_addr_str,
                    &already_synced_kc_ids,
                )
                .await
                .map_err(|e| format!("Failed to remove already-synced KCs: {}", e))?;
        }

        let mut synced = already_synced_kc_ids.len() as u64;

        if kcs_to_sync.is_empty() {
            return Ok(ContractSyncResult {
                enqueued,
                pending,
                synced,
                failed: 0,
            });
        }

        // Step 5: For KCs not found locally and not expired, batch GET from network
        let (fetched_kcs, failed_kc_ids) = self
            .fetch_kcs_from_network(blockchain_id, &kcs_to_sync)
            .await;

        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            fetched = fetched_kcs.len(),
            failed = failed_kc_ids.len(),
            "[DKG SYNC] Network fetch completed for contract"
        );

        // Step 6: Check expiration and store fetched KCs in triple store
        let mut failed_kc_ids = failed_kc_ids;
        let mut successfully_synced_kc_ids: Vec<u64> = Vec::new();
        let mut expired_kc_ids: Vec<u64> = Vec::new();

        // Get current epoch once for expiration checks
        let current_epoch = self
            .blockchain_manager
            .get_current_epoch(blockchain_id)
            .await
            .ok();

        // Build a lookup from UAL to KC info
        let ual_to_kc: HashMap<&str, &KcToSync> =
            kcs_to_sync.iter().map(|kc| (kc.ual.as_str(), kc)).collect();

        for (ual, fetch_result) in &fetched_kcs {
            let Some(kc) = ual_to_kc.get(ual.as_str()) else {
                continue;
            };

            // Check if KC is expired before inserting
            if let Some(current_epoch) = current_epoch {
                match self
                    .blockchain_manager
                    .get_kc_end_epoch(blockchain_id, contract_address, kc.kc_id as u128)
                    .await
                {
                    Ok(end_epoch) => {
                        if current_epoch > end_epoch {
                            tracing::debug!(
                                blockchain_id = %blockchain_id,
                                contract = %contract_addr_str,
                                kc_id = kc.kc_id,
                                current_epoch,
                                end_epoch,
                                "[DKG SYNC] KC is expired, skipping insert"
                            );
                            expired_kc_ids.push(kc.kc_id);
                            continue;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            blockchain_id = %blockchain_id,
                            contract = %contract_addr_str,
                            kc_id = kc.kc_id,
                            error = %e,
                            "[DKG SYNC] Failed to get KC end epoch, will retry later"
                        );
                        if !failed_kc_ids.contains(&kc.kc_id) {
                            failed_kc_ids.push(kc.kc_id);
                        }
                        continue;
                    }
                }
            }

            // Parse metadata from network response (optional)
            let metadata: Option<KnowledgeCollectionMetadata> = fetch_result
                .metadata
                .as_ref()
                .and_then(|triples| parse_metadata_from_triples(triples));

            if metadata.is_none() {
                tracing::debug!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    kc_id = kc.kc_id,
                    "[DKG SYNC] No metadata from network, storing assertion without metadata"
                );
                counter!("sync_metadata_source", "blockchain" => blockchain_id.as_str().to_string(), "source" => "none")
                    .increment(1);
            } else {
                counter!("sync_metadata_source", "blockchain" => blockchain_id.as_str().to_string(), "source" => "network")
                    .increment(1);
            }

            // Insert into triple store
            match self
                .triple_store_service
                .insert_knowledge_collection(ual, &fetch_result.assertion, &metadata)
                .await
            {
                Ok(triples_inserted) => {
                    tracing::debug!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc.kc_id,
                        ual = %ual,
                        triples = triples_inserted,
                        "[DKG SYNC] Stored synced KC in triple store"
                    );
                    successfully_synced_kc_ids.push(kc.kc_id);
                }
                Err(e) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc.kc_id,
                        error = %e,
                        "[DKG SYNC] Failed to store KC in triple store"
                    );
                    if !failed_kc_ids.contains(&kc.kc_id) {
                        failed_kc_ids.push(kc.kc_id);
                    }
                }
            }
        }

        // Record expired KCs metric
        if !expired_kc_ids.is_empty() {
            counter!("sync_expired_total", "blockchain" => blockchain_id.as_str().to_string())
                .increment(expired_kc_ids.len() as u64);
        }

        // Step 7: Update DB - remove synced/expired KCs, increment retry_count for failed ones
        // Remove expired KCs from queue (they should not be retried)
        if !expired_kc_ids.is_empty()
            && let Err(e) = self
                .repository_manager
                .kc_sync_repository()
                .remove_kcs(blockchain_id.as_str(), &contract_addr_str, &expired_kc_ids)
                .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                "[DKG SYNC] Failed to remove expired KCs from queue"
            );
        }
        if !successfully_synced_kc_ids.is_empty() {
            if let Err(e) = self
                .repository_manager
                .kc_sync_repository()
                .remove_kcs(
                    blockchain_id.as_str(),
                    &contract_addr_str,
                    &successfully_synced_kc_ids,
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
            synced += successfully_synced_kc_ids.len() as u64;
        }

        if !failed_kc_ids.is_empty()
            && let Err(e) = self
                .repository_manager
                .kc_sync_repository()
                .increment_retry_count(blockchain_id.as_str(), &contract_addr_str, &failed_kc_ids)
                .await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                contract = %contract_addr_str,
                error = %e,
                "[DKG SYNC] Failed to increment retry count for failed KCs"
            );
        }

        Ok(ContractSyncResult {
            enqueued,
            pending,
            synced,
            failed: failed_kc_ids.len() as u64,
        })
    }

    /// Filter pending KCs: check which already exist locally.
    /// Expiration is checked later, after fetching from network (to avoid slow RPC calls).
    /// Returns (kcs_to_sync, already_synced_kc_ids)
    async fn filter_pending_kcs(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        contract_addr_str: &str,
        pending_kc_ids: &[u64],
    ) -> (Vec<KcToSync>, Vec<u64>) {
        let blockchain_label = blockchain_id.as_str();
        let mut kcs_to_sync = Vec::new();
        let mut already_synced_kc_ids = Vec::new();
        let mut skipped_due_to_errors = 0u64;

        let total_kcs = pending_kc_ids.len();
        tracing::debug!(
            blockchain_id = %blockchain_id,
            contract = %contract_addr_str,
            total_kcs,
            "[DKG SYNC] Starting filter step"
        );

        for (idx, &kc_id) in pending_kc_ids.iter().enumerate() {
            // Log progress every 10 KCs
            if idx > 0 && idx % 10 == 0 {
                tracing::debug!(
                    blockchain_id = %blockchain_id,
                    progress = format!("{}/{}", idx, total_kcs),
                    "[DKG SYNC] Filter progress"
                );
            }

            // Get token ID range for the KC
            let token_range = match self
                .blockchain_manager
                .get_knowledge_assets_range(blockchain_id, contract_address, kc_id as u128)
                .await
            {
                Ok(Some(range)) => range,
                Ok(None) => {
                    // KC doesn't exist on chain
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc_id,
                        "[DKG SYNC] KC has no token range on chain, skipping"
                    );
                    skipped_due_to_errors += 1;
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        kc_id = kc_id,
                        error = %e,
                        "[DKG SYNC] Failed to get KC token range, will retry later"
                    );
                    skipped_due_to_errors += 1;
                    continue;
                }
            };

            let (global_start, global_end, global_burned) = token_range;

            // Convert global token IDs to local 1-based indices
            // Global: (kc_id - 1) * 1_000_000 + local_id
            // Local: global - (kc_id - 1) * 1_000_000
            let offset = (kc_id - 1) * MAX_TOKENS_PER_KC;
            let start_token_id = global_start.saturating_sub(offset);
            let end_token_id = global_end.saturating_sub(offset);
            let burned: Vec<u64> = global_burned
                .into_iter()
                .map(|b| b.saturating_sub(offset))
                .collect();

            // Build UAL to check local existence
            let kc_ual = derive_ual(blockchain_id, &contract_address, kc_id as u128, None);

            // Check if KC already exists locally
            let exists_locally = self
                .triple_store_service
                .knowledge_collection_exists(&kc_ual, start_token_id, end_token_id)
                .await;

            if exists_locally {
                // KC already exists locally
                already_synced_kc_ids.push(kc_id);
                continue;
            }

            // KC needs to be synced
            kcs_to_sync.push(KcToSync {
                kc_id,
                ual: kc_ual,
                start_token_id,
                end_token_id,
                burned,
            });
        }

        // Record filter metrics
        counter!("sync_filter_already_synced_total", "blockchain" => blockchain_label.to_string())
            .increment(already_synced_kc_ids.len() as u64);
        counter!("sync_filter_needs_sync_total", "blockchain" => blockchain_label.to_string())
            .increment(kcs_to_sync.len() as u64);
        counter!("sync_filter_skipped_errors_total", "blockchain" => blockchain_label.to_string())
            .increment(skipped_due_to_errors);

        (kcs_to_sync, already_synced_kc_ids)
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

    /// Fetch KCs from the network by sending batch GET requests to peers.
    ///
    /// This function:
    /// 1. Splits KCs into chunks of BATCH_GET_UAL_MAX_LIMIT
    /// 2. For each chunk, queries peers in batches of BATCH_SIZE (5 peers at a time)
    /// 3. Validates responses and aggregates successful results
    ///
    /// Returns a map of UAL -> (Assertion, metadata) for successfully fetched KCs,
    /// and a list of KC IDs that could not be fetched.
    async fn fetch_kcs_from_network(
        &self,
        blockchain_id: &BlockchainId,
        kcs_to_sync: &[KcToSync],
    ) -> (HashMap<String, NetworkFetchResult>, Vec<u64>) {
        let mut fetched: HashMap<String, NetworkFetchResult> = HashMap::new();
        let mut failed_kc_ids: Vec<u64> = Vec::new();

        if kcs_to_sync.is_empty() {
            return (fetched, failed_kc_ids);
        }

        let blockchain_label = blockchain_id.as_str();

        // Get shard nodes (peers) for this blockchain
        let peers = match self.get_shard_peers(blockchain_id).await {
            Ok(peers) => peers,
            Err(e) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    error = %e,
                    "[DKG SYNC] Failed to get shard peers, marking all KCs as failed"
                );
                counter!("sync_network_no_peers_total", "blockchain" => blockchain_label.to_string(), "reason" => "error")
                    .increment(1);
                failed_kc_ids = kcs_to_sync.iter().map(|kc| kc.kc_id).collect();
                return (fetched, failed_kc_ids);
            }
        };

        if peers.is_empty() {
            tracing::warn!(
                blockchain_id = %blockchain_id,
                "[DKG SYNC] No peers available for network sync"
            );
            counter!("sync_network_no_peers_total", "blockchain" => blockchain_label.to_string(), "reason" => "empty")
                .increment(1);
            failed_kc_ids = kcs_to_sync.iter().map(|kc| kc.kc_id).collect();
            return (fetched, failed_kc_ids);
        }

        gauge!("sync_network_peers_available", "blockchain" => blockchain_label.to_string())
            .set(peers.len() as f64);

        tracing::info!(
            blockchain_id = %blockchain_id,
            kc_count = kcs_to_sync.len(),
            peer_count = peers.len(),
            "[DKG SYNC] Starting network fetch for KCs"
        );

        // Split KCs into chunks of BATCH_GET_UAL_MAX_LIMIT
        for chunk in kcs_to_sync.chunks(BATCH_GET_UAL_MAX_LIMIT) {
            let chunk_result = self
                .fetch_kc_chunk_from_network(blockchain_id, chunk, &peers)
                .await;

            // Merge results
            for (ual, result) in chunk_result.fetched {
                fetched.insert(ual, result);
            }
            failed_kc_ids.extend(chunk_result.failed_kc_ids);
        }

        counter!("sync_network_fetched_total", "blockchain" => blockchain_label.to_string())
            .increment(fetched.len() as u64);
        counter!("sync_network_fetch_failed_total", "blockchain" => blockchain_label.to_string())
            .increment(failed_kc_ids.len() as u64);

        tracing::info!(
            blockchain_id = %blockchain_id,
            fetched = fetched.len(),
            failed = failed_kc_ids.len(),
            "[DKG SYNC] Network fetch completed"
        );

        (fetched, failed_kc_ids)
    }

    /// Fetch a single chunk of KCs from the network.
    async fn fetch_kc_chunk_from_network(
        &self,
        blockchain_id: &BlockchainId,
        kcs: &[KcToSync],
        peers: &[PeerId],
    ) -> ChunkFetchResult {
        let blockchain_label = blockchain_id.as_str();
        let mut fetched: HashMap<String, NetworkFetchResult> = HashMap::new();
        let mut uals_still_needed: Vec<String> = kcs.iter().map(|kc| kc.ual.clone()).collect();

        // Build lookup maps
        let ual_to_kc: HashMap<&str, &KcToSync> =
            kcs.iter().map(|kc| (kc.ual.as_str(), kc)).collect();

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
                let token_ids =
                    TokenIds::new(kc.start_token_id, kc.end_token_id, kc.burned.clone());
                (kc.ual.clone(), token_ids)
            })
            .collect();

        // Build the batch request
        let batch_request = BatchGetRequestData::new(
            blockchain_id.to_string(),
            uals_still_needed.clone(),
            token_ids_map,
            true, // include_metadata
        );

        // Send requests to peers in batches
        for (batch_idx, peer_batch) in peers.chunks(BatchGetOperation::BATCH_SIZE).enumerate() {
            if uals_still_needed.is_empty() {
                break;
            }

            tracing::debug!(
                blockchain_id = %blockchain_id,
                batch = batch_idx,
                peers_in_batch = peer_batch.len(),
                uals_needed = uals_still_needed.len(),
                "[DKG SYNC] Sending batch request to peers"
            );

            // Send requests concurrently to all peers in this batch
            let request_futures: Vec<_> = peer_batch
                .iter()
                .map(|peer| {
                    let peer = *peer;
                    let request_data = batch_request.clone();
                    tracing::info!(req = ?request_data,"sending batch_get request");
                    let network_manager = Arc::clone(&self.network_manager);
                    // Generate a unique operation ID for tracking (not stored, just for the
                    // request)
                    let operation_id = uuid::Uuid::new_v4();
                    async move {
                        // Get peer addresses from Kademlia for reliable request delivery
                        let addresses = network_manager
                            .get_peer_addresses(peer)
                            .await
                            .unwrap_or_default();
                        let result = network_manager
                            .send_batch_get_request(peer, addresses, operation_id, request_data)
                            .await;
                        (peer, result)
                    }
                })
                .collect();

            let results = join_all(request_futures).await;

            // Process responses
            for (peer, result) in results {
                match result {
                    Ok(response) => {
                        counter!("sync_peer_request_total", "blockchain" => blockchain_label.to_string(), "result" => "success")
                            .increment(1);
                        let metadata_map = response.metadata();

                        for (ual, assertion) in response.assertions() {
                            // Skip if already satisfied
                            if !uals_still_needed.contains(ual) {
                                continue;
                            }

                            // Skip empty assertions - peer doesn't have data
                            if !assertion.has_data() {
                                counter!("sync_validation_total", "blockchain" => blockchain_label.to_string(), "result" => "empty")
                                    .increment(1);
                                tracing::trace!(
                                    ual = %ual,
                                    peer = %peer,
                                    "[DKG SYNC] Peer returned empty assertion, skipping"
                                );
                                continue;
                            }

                            // Validate the response
                            if let Some(parsed_ual) = parsed_uals.get(ual) {
                                let is_valid = self
                                    .get_validation_service
                                    .validate_response(assertion, parsed_ual, Visibility::All)
                                    .await;

                                if is_valid {
                                    counter!("sync_validation_total", "blockchain" => blockchain_label.to_string(), "result" => "valid")
                                        .increment(1);
                                    let metadata = metadata_map.get(ual).cloned();
                                    fetched.insert(
                                        ual.clone(),
                                        NetworkFetchResult {
                                            assertion: assertion.clone(),
                                            metadata,
                                        },
                                    );
                                    uals_still_needed.retain(|u| u != ual);

                                    tracing::debug!(
                                        ual = %ual,
                                        peer = %peer,
                                        "[DKG SYNC] Received and validated KC from network"
                                    );
                                } else {
                                    counter!("sync_validation_total", "blockchain" => blockchain_label.to_string(), "result" => "invalid")
                                        .increment(1);
                                    tracing::debug!(
                                        ual = %ual,
                                        peer = %peer,
                                        "[DKG SYNC] Response validation failed"
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        counter!("sync_peer_request_total", "blockchain" => blockchain_label.to_string(), "result" => "error")
                            .increment(1);
                        tracing::debug!(
                            peer = %peer,
                            error = %e,
                            "[DKG SYNC] Request to peer failed"
                        );
                    }
                }
            }
        }

        // Map remaining UALs back to KC IDs for the failed list
        let failed_kc_ids: Vec<u64> = uals_still_needed
            .iter()
            .filter_map(|ual| ual_to_kc.get(ual.as_str()).map(|kc| kc.kc_id))
            .collect();

        ChunkFetchResult {
            fetched,
            failed_kc_ids,
        }
    }

    /// Get shard peers for the given blockchain, excluding self.
    async fn get_shard_peers(&self, blockchain_id: &BlockchainId) -> Result<Vec<PeerId>, String> {
        let shard_nodes = self
            .repository_manager
            .shard_repository()
            .get_all_peer_records(blockchain_id.as_str())
            .await
            .map_err(|e| format!("Failed to get shard nodes: {}", e))?;

        let my_peer_id = *self.network_manager.peer_id();
        let peers: Vec<PeerId> = shard_nodes
            .iter()
            .filter_map(|record| record.peer_id.parse().ok())
            .filter(|peer_id| *peer_id != my_peer_id)
            .collect();

        Ok(peers)
    }
}

/// Result of fetching a single KC from the network.
struct NetworkFetchResult {
    assertion: Assertion,
    metadata: Option<Vec<String>>,
}

/// Result of fetching a chunk of KCs from the network.
struct ChunkFetchResult {
    fetched: HashMap<String, NetworkFetchResult>,
    failed_kc_ids: Vec<u64>,
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
        let sync_start = Instant::now();
        let blockchain_label = data.blockchain_id.as_str();

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

        // Record sync metrics
        counter!("sync_kcs_enqueued_total", "blockchain" => blockchain_label.to_string())
            .increment(total_enqueued);
        gauge!("sync_kcs_pending", "blockchain" => blockchain_label.to_string())
            .set(total_pending as f64);
        counter!("sync_kcs_synced_total", "blockchain" => blockchain_label.to_string())
            .increment(total_synced);
        counter!("sync_kcs_failed_total", "blockchain" => blockchain_label.to_string())
            .increment(total_failed);
        histogram!("sync_cycle_duration_seconds", "blockchain" => blockchain_label.to_string())
            .record(sync_start.elapsed().as_secs_f64());

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
