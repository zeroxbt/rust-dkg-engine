//! Proving command handler implementation.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::U256;
use chrono::Utc;
use futures::{StreamExt, stream::FuturesUnordered};
use libp2p::PeerId;
use uuid::Uuid;

use super::{PROVING_PERIOD, REORG_BUFFER};
use crate::{
    commands::{command_executor::CommandExecutionResult, command_registry::CommandHandler},
    context::Context,
    managers::{
        blockchain::BlockchainManager,
        network::{
            NetworkError, NetworkManager,
            message::ResponseBody,
            messages::{GetRequestData, GetResponseData},
        },
        repository::{ChallengeState, RepositoryManager},
        triple_store::{
            Assertion, TokenIds, group_nquads_by_subject,
            query::subjects::PRIVATE_HASH_SUBJECT_PREFIX,
        },
    },
    operations::protocols,
    services::{GetValidationService, PeerPerformanceTracker, TripleStoreService},
    types::{BlockchainId, ParsedUal, Visibility, derive_ual},
    utils::validation,
};

#[derive(Clone)]
pub(crate) struct ProvingCommandData {
    pub blockchain_id: BlockchainId,
}

impl ProvingCommandData {
    pub(crate) fn new(blockchain_id: BlockchainId) -> Self {
        Self { blockchain_id }
    }
}

pub(crate) struct ProvingCommandHandler {
    blockchain_manager: Arc<BlockchainManager>,
    repository_manager: Arc<RepositoryManager>,
    triple_store_service: Arc<TripleStoreService>,
    network_manager: Arc<NetworkManager>,
    get_validation_service: Arc<GetValidationService>,
    peer_performance_tracker: Arc<PeerPerformanceTracker>,
}

impl ProvingCommandHandler {
    pub(crate) fn new(context: Arc<Context>) -> Self {
        Self {
            blockchain_manager: Arc::clone(context.blockchain_manager()),
            repository_manager: Arc::clone(context.repository_manager()),
            triple_store_service: Arc::clone(context.triple_store_service()),
            network_manager: Arc::clone(context.network_manager()),
            get_validation_service: Arc::clone(context.get_validation_service()),
            peer_performance_tracker: Arc::clone(context.peer_performance_tracker()),
        }
    }

    /// Check if node is part of the shard for this blockchain.
    async fn is_in_shard(&self, blockchain_id: &BlockchainId) -> bool {
        let peer_id = self.network_manager.peer_id().to_string();

        self.repository_manager
            .shard_repository()
            .get_peer_record(blockchain_id.as_str(), &peer_id)
            .await
            .ok()
            .flatten()
            .is_some()
    }

    /// Get the identity ID for this blockchain.
    fn identity_id(&self, blockchain_id: &BlockchainId) -> u128 {
        self.blockchain_manager.identity_id(blockchain_id)
    }

    /// Prepare quads for Merkle proof calculation.
    /// Groups by subject, separates private-hash triples, sorts each group.
    fn prepare_quads_for_proof(public_triples: &[String]) -> Vec<String> {
        let private_hash_prefix = format!("<{}", PRIVATE_HASH_SUBJECT_PREFIX);

        let mut filtered_public: Vec<&str> = Vec::new();
        let mut private_hash_triples: Vec<&str> = Vec::new();

        for triple in public_triples {
            if triple.starts_with(&private_hash_prefix) {
                private_hash_triples.push(triple);
            } else {
                filtered_public.push(triple);
            }
        }

        // Group by subject, then append private-hash groups
        let mut grouped = group_nquads_by_subject(&filtered_public);
        grouped.extend(group_nquads_by_subject(&private_hash_triples));

        // Sort each group and flatten
        grouped
            .iter()
            .flat_map(|group| {
                let mut sorted_group: Vec<&str> = group.to_vec();
                sorted_group.sort();
                sorted_group.into_iter().map(String::from)
            })
            .collect()
    }

    /// Load shard peers for the given blockchain.
    async fn load_shard_peers(&self, blockchain_id: &BlockchainId) -> Vec<PeerId> {
        let shard_nodes = match self
            .repository_manager
            .shard_repository()
            .get_all_peer_records(blockchain_id.as_str())
            .await
        {
            Ok(nodes) => nodes,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get shard nodes");
                return Vec::new();
            }
        };

        // Filter out self and parse peer IDs
        let my_peer_id = *self.network_manager.peer_id();
        shard_nodes
            .iter()
            .filter_map(|record| record.peer_id.parse().ok())
            .filter(|peer_id| *peer_id != my_peer_id)
            .collect()
    }

    /// Fetch assertion from network peers.
    ///
    /// Uses the same concurrent request pattern as the GET command:
    /// - Sort peers by latency
    /// - Send concurrent requests
    /// - Track success/failure for peer performance
    /// - Return on first valid response (proving only needs 1)
    #[tracing::instrument(
        name = "proving.network_fetch",
        skip(self, parsed_ual, token_ids),
        fields(
            ual = %parsed_ual.to_ual_string(),
            peer_count = tracing::field::Empty,
        )
    )]
    async fn fetch_from_network(
        &self,
        parsed_ual: &ParsedUal,
        token_ids: TokenIds,
    ) -> Option<Assertion> {
        let mut peers = self.load_shard_peers(&parsed_ual.blockchain).await;

        if peers.is_empty() {
            tracing::warn!("No peers available in shard");
            return None;
        }

        tracing::Span::current().record("peer_count", tracing::field::display(peers.len()));

        // Sort by latency (best performers first)
        self.peer_performance_tracker.sort_by_latency(&mut peers);

        tracing::debug!(
            total_peers = peers.len(),
            "Selected peers for network query"
        );

        // Build request data
        let request_data = GetRequestData::new(
            parsed_ual.blockchain.clone(),
            format!("{:?}", &parsed_ual.contract),
            parsed_ual.knowledge_collection_id,
            parsed_ual.knowledge_asset_id,
            parsed_ual.to_ual_string(),
            token_ids,
            false, // no metadata needed for proving
            None,  // no paranet
        );

        // Use a random operation ID for tracking
        let operation_id = Uuid::new_v4();

        // Concurrent request pattern (same as GET command)
        let mut futures = FuturesUnordered::new();
        let mut peers_iter = peers.iter().cloned();
        let limit = protocols::get::CONCURRENT_PEERS.max(1).min(peers.len());

        // Start initial batch of concurrent requests
        for _ in 0..limit {
            if let Some(peer) = peers_iter.next() {
                futures.push(self.send_get_request_to_peer(
                    peer,
                    operation_id,
                    request_data.clone(),
                    parsed_ual.clone(),
                ));
            }
        }

        let mut success_count = 0u16;
        let mut failure_count = 0u16;

        // Process responses as they arrive
        while let Some((peer, outcome, elapsed, assertion)) = futures.next().await {
            match outcome {
                Ok(true) => {
                    success_count += 1;
                    self.peer_performance_tracker.record_latency(&peer, elapsed);

                    tracing::info!(
                        peer = %peer,
                        elapsed_ms = elapsed.as_millis(),
                        "Got valid assertion from network"
                    );

                    // For proving, we only need 1 valid response
                    return assertion;
                }
                Ok(false) => {
                    failure_count += 1;
                    tracing::debug!(
                        peer = %peer,
                        "Response validation failed"
                    );
                }
                Err(e) => {
                    failure_count += 1;
                    self.peer_performance_tracker.record_failure(&peer);
                    tracing::debug!(
                        peer = %peer,
                        error = %e,
                        "Request to peer failed"
                    );
                }
            }

            // Queue next peer if available
            if let Some(peer) = peers_iter.next() {
                futures.push(self.send_get_request_to_peer(
                    peer,
                    operation_id,
                    request_data.clone(),
                    parsed_ual.clone(),
                ));
            }
        }

        tracing::warn!(
            success_count,
            failure_count,
            "Failed to fetch assertion from any peer"
        );
        None
    }

    /// Send GET request to a single peer and validate response.
    async fn send_get_request_to_peer(
        &self,
        peer: PeerId,
        operation_id: Uuid,
        request_data: GetRequestData,
        parsed_ual: ParsedUal,
    ) -> (
        PeerId,
        Result<bool, NetworkError>,
        Duration,
        Option<Assertion>,
    ) {
        let start = Instant::now();
        let addresses = self
            .network_manager
            .get_peer_addresses(peer)
            .await
            .unwrap_or_default();

        let result = self
            .network_manager
            .send_get_request(peer, addresses, operation_id, request_data)
            .await;

        let elapsed = start.elapsed();

        match result {
            Ok(response) => {
                let assertion = self
                    .validate_response(&response, &parsed_ual, Visibility::Public)
                    .await;
                let is_valid = assertion.is_some();
                (peer, Ok(is_valid), elapsed, assertion)
            }
            Err(e) => (peer, Err(e), elapsed, None),
        }
    }

    /// Validate a GET response and extract the assertion if valid.
    async fn validate_response(
        &self,
        response: &GetResponseData,
        parsed_ual: &ParsedUal,
        visibility: Visibility,
    ) -> Option<Assertion> {
        match response {
            ResponseBody::Ack(ack) => {
                let assertion = &ack.assertion;

                // Validate the assertion using the same service as GET command
                let is_valid = self
                    .get_validation_service
                    .validate_response(assertion, parsed_ual, visibility)
                    .await;

                if !is_valid {
                    return None;
                }

                Some(Assertion::new(
                    assertion.public.clone(),
                    assertion.private.clone(),
                ))
            }
            ResponseBody::Error(err) => {
                tracing::debug!(error = %err.error_message, "Peer returned error");
                None
            }
        }
    }
}

impl CommandHandler<ProvingCommandData> for ProvingCommandHandler {
    #[tracing::instrument(
        name = "periodic.proving",
        skip(self, data),
        fields(blockchain_id = %data.blockchain_id)
    )]
    async fn execute(&self, data: &ProvingCommandData) -> CommandExecutionResult {
        // 1. Check if we're in the shard
        if !self.is_in_shard(&data.blockchain_id).await {
            tracing::debug!("Node not in shard, skipping proving");
            return CommandExecutionResult::Repeat {
                delay: PROVING_PERIOD,
            };
        }

        let identity_id = self.identity_id(&data.blockchain_id);

        // 2. Get active proof period status
        let proof_period = match self
            .blockchain_manager
            .get_active_proof_period_status(&data.blockchain_id)
            .await
        {
            Ok(status) => status,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get proof period status");
                return CommandExecutionResult::Repeat {
                    delay: PROVING_PERIOD,
                };
            }
        };

        // 3. Get latest challenge from database
        let latest_challenge = self
            .repository_manager
            .proof_challenge_repository()
            .get_latest(data.blockchain_id.as_str())
            .await
            .ok()
            .flatten();

        let current_start_block = proof_period.active_proof_period_start_block;

        if !proof_period.is_valid {
            tracing::debug!(
                active_start_block = %proof_period.active_proof_period_start_block,
                "No valid proof period active"
            );
        }

        // Check if we have a challenge for the current period
        if proof_period.is_valid
            && let Some(ref challenge) = latest_challenge
        {
            let challenge_start_block = U256::from(challenge.proof_period_start_block as u64);

            if challenge_start_block == current_start_block {
                let state = ChallengeState::from_str(&challenge.state);

                match state {
                    ChallengeState::Finalized => {
                        // Already finalized, nothing to do
                        tracing::debug!("Challenge already finalized");
                        return CommandExecutionResult::Repeat {
                            delay: PROVING_PERIOD,
                        };
                    }
                    ChallengeState::Submitted => {
                        // Check score on-chain first
                        let score = self
                            .blockchain_manager
                            .get_node_epoch_proof_period_score(
                                &data.blockchain_id,
                                identity_id,
                                U256::from(challenge.epoch as u64),
                                challenge_start_block,
                            )
                            .await;

                        match score {
                            Ok(score) if score > U256::ZERO => {
                                // Score is positive - wait for reorg buffer before finalizing
                                let updated_at = challenge.updated_at;
                                let now = Utc::now().timestamp();
                                let elapsed = Duration::from_secs((now - updated_at).max(0) as u64);

                                if elapsed < REORG_BUFFER {
                                    tracing::debug!(
                                        elapsed_secs = elapsed.as_secs(),
                                        buffer_secs = REORG_BUFFER.as_secs(),
                                        "Waiting for reorg buffer before finalizing"
                                    );
                                    return CommandExecutionResult::Repeat {
                                        delay: PROVING_PERIOD,
                                    };
                                }

                                // Finalize
                                if let Err(e) = self
                                    .repository_manager
                                    .proof_challenge_repository()
                                    .set_state(
                                        data.blockchain_id.as_str(),
                                        challenge.epoch,
                                        challenge.proof_period_start_block,
                                        ChallengeState::Finalized,
                                        Some(score.to_string()),
                                    )
                                    .await
                                {
                                    tracing::warn!(error = %e, "Failed to update challenge state");
                                }
                                tracing::info!(score = %score, "Proof finalized successfully");
                                return CommandExecutionResult::Repeat {
                                    delay: PROVING_PERIOD,
                                };
                            }
                            Ok(_) => {
                                // Score is zero - reset to Pending and fall through to
                                // re-submit
                                tracing::warn!("Score is zero, resetting challenge to retry");
                                if let Err(e) = self
                                    .repository_manager
                                    .proof_challenge_repository()
                                    .set_state(
                                        data.blockchain_id.as_str(),
                                        challenge.epoch,
                                        challenge.proof_period_start_block,
                                        ChallengeState::Pending,
                                        None,
                                    )
                                    .await
                                {
                                    tracing::warn!(error = %e, "Failed to reset challenge state");
                                }
                                // Fall through to re-submit proof
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Failed to get proof score");
                                return CommandExecutionResult::Repeat {
                                    delay: PROVING_PERIOD,
                                };
                            }
                        }
                    }
                    ChallengeState::Pending => {
                        // Need to submit proof - fall through to submission logic
                    }
                }
            }
            // Different period - need new challenge
        }

        // 4. Create new challenge if needed
        let needs_new_challenge = if !proof_period.is_valid {
            true
        } else {
            latest_challenge
                .as_ref()
                .map(|c| U256::from(c.proof_period_start_block as u64) != current_start_block)
                .unwrap_or(true)
        };

        if needs_new_challenge
            && let Err(e) = self
                .blockchain_manager
                .create_challenge(&data.blockchain_id)
                .await
        {
            tracing::warn!(error = %e, "Failed to create challenge");
            return CommandExecutionResult::Repeat {
                delay: PROVING_PERIOD,
            };
        }

        // 5. Get challenge details from chain
        let node_challenge = match self
            .blockchain_manager
            .get_node_challenge(&data.blockchain_id, identity_id)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get node challenge");
                return CommandExecutionResult::Repeat {
                    delay: PROVING_PERIOD,
                };
            }
        };

        let epoch = node_challenge.epoch.as_limbs()[0] as i64;
        let start_block = node_challenge.active_proof_period_start_block.as_limbs()[0] as i64;
        let kc_id = node_challenge.knowledge_collection_id.as_limbs()[0] as u128;
        let chunk_index = node_challenge.chunk_id.as_limbs()[0] as usize;
        let contract_addr =
            format!("{:?}", node_challenge.knowledge_collection_storage_contract).to_lowercase();

        // Store challenge in database if new
        let db_challenge = self
            .repository_manager
            .proof_challenge_repository()
            .get_latest(data.blockchain_id.as_str())
            .await
            .ok()
            .flatten();

        let needs_db_record = db_challenge
            .as_ref()
            .map(|c| c.proof_period_start_block != start_block)
            .unwrap_or(true);

        if needs_db_record
            && let Err(e) = self
                .repository_manager
                .proof_challenge_repository()
                .create(
                    data.blockchain_id.as_str(),
                    epoch,
                    start_block,
                    &contract_addr,
                    kc_id as i64,
                    chunk_index as i64,
                )
                .await
        {
            tracing::warn!(error = %e, "Failed to store challenge");
            // Continue anyway - we can still try to submit
        }

        // 6. Fetch assertion
        let ual = derive_ual(
            &data.blockchain_id,
            &node_challenge.knowledge_collection_storage_contract,
            kc_id,
            None,
        );

        let parsed_ual = match crate::types::parse_ual(&ual) {
            Ok(u) => u,
            Err(e) => {
                tracing::warn!(error = %e, ual = %ual, "Failed to parse UAL");
                return CommandExecutionResult::Repeat {
                    delay: PROVING_PERIOD,
                };
            }
        };

        // Get token IDs range from blockchain
        let token_ids = match self
            .blockchain_manager
            .get_knowledge_assets_range(
                &data.blockchain_id,
                node_challenge.knowledge_collection_storage_contract,
                kc_id,
            )
            .await
        {
            Ok(Some((global_start, global_end, global_burned))) => {
                TokenIds::from_global_range(kc_id, global_start, global_end, global_burned)
            }
            Ok(None) => {
                // Fallback for old ContentAssetStorage contracts
                TokenIds::single(1)
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get knowledge assets range");
                return CommandExecutionResult::Repeat {
                    delay: PROVING_PERIOD,
                };
            }
        };

        // Try local triple store first (same as GET command's local_query_phase)
        let assertion = match self
            .triple_store_service
            .query_assertion(&parsed_ual, &token_ids, Visibility::Public, false)
            .await
        {
            Ok(Some(result)) if result.assertion.has_data() => {
                // Validate local data
                let is_valid = self
                    .get_validation_service
                    .validate_response(&result.assertion, &parsed_ual, Visibility::Public)
                    .await;

                if is_valid {
                    tracing::debug!("Found valid assertion locally");
                    result.assertion
                } else {
                    tracing::debug!("Local validation failed, trying network");
                    match self
                        .fetch_from_network(&parsed_ual, token_ids.clone())
                        .await
                    {
                        Some(assertion) => assertion,
                        None => {
                            tracing::warn!(ual = %ual, "Assertion not found on network");
                            return CommandExecutionResult::Repeat {
                                delay: PROVING_PERIOD,
                            };
                        }
                    }
                }
            }
            Ok(_) => {
                // Not found locally - try network
                tracing::debug!(ual = %ual, "Assertion not found locally, trying network");
                match self
                    .fetch_from_network(&parsed_ual, token_ids.clone())
                    .await
                {
                    Some(assertion) => assertion,
                    None => {
                        tracing::warn!(ual = %ual, "Assertion not found locally or on network");
                        return CommandExecutionResult::Repeat {
                            delay: PROVING_PERIOD,
                        };
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to query local triple store, trying network");
                match self
                    .fetch_from_network(&parsed_ual, token_ids.clone())
                    .await
                {
                    Some(assertion) => assertion,
                    None => {
                        tracing::warn!(ual = %ual, "Assertion not found on network");
                        return CommandExecutionResult::Repeat {
                            delay: PROVING_PERIOD,
                        };
                    }
                }
            }
        };

        // 7. Calculate Merkle proof
        let prepared_quads = Self::prepare_quads_for_proof(&assertion.public);

        let proof_result = match validation::calculate_merkle_proof(&prepared_quads, chunk_index) {
            Ok(result) => result,
            Err(e) => {
                tracing::warn!(error = %e, chunk_index, "Failed to calculate Merkle proof");
                return CommandExecutionResult::Repeat {
                    delay: PROVING_PERIOD,
                };
            }
        };

        // Convert B256 to [u8; 32]
        let proof_bytes: Vec<[u8; 32]> = proof_result.proof.iter().map(|b| *b.as_ref()).collect();

        // 8. Submit proof
        if let Err(e) = self
            .blockchain_manager
            .submit_proof(&data.blockchain_id, &proof_result.chunk, &proof_bytes)
            .await
        {
            tracing::warn!(error = %e, "Failed to submit proof");
            return CommandExecutionResult::Repeat {
                delay: PROVING_PERIOD,
            };
        }

        // 9. Update database state
        if let Err(e) = self
            .repository_manager
            .proof_challenge_repository()
            .set_state(
                data.blockchain_id.as_str(),
                epoch,
                start_block,
                ChallengeState::Submitted,
                None,
            )
            .await
        {
            tracing::warn!(error = %e, "Failed to update challenge state");
        }

        tracing::info!(epoch, chunk_index, "Proof submitted successfully");

        CommandExecutionResult::Repeat {
            delay: PROVING_PERIOD,
        }
    }
}
