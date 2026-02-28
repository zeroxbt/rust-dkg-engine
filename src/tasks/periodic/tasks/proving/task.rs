//! Proving periodic task implementation.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use dkg_blockchain::{BlockchainManager, U256};
use dkg_domain::{Assertion, BlockchainId, ParsedUal, TokenIds, Visibility, derive_ual};
use dkg_network::{GetRequestData, NetworkManager, PeerId, STREAM_PROTOCOL_GET};
use dkg_observability as observability;
use dkg_repository::{ChallengeState, ProofChallengeRepository};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{PROVING_PERIOD, REORG_BUFFER};
use crate::{
    application::{
        AssertionValidation, TokenRangeResolutionPolicy, TripleStoreAssertions,
        fetch_assertion_from_local,
        get_assertion::{network_fetch, resolve_token_ids},
        group_and_sort_public_triples,
    },
    node_state::PeerRegistry,
    tasks::periodic::ProvingDeps,
    tasks::periodic::runner::run_with_shutdown,
};

pub(crate) struct ProvingTask {
    blockchain_manager: Arc<BlockchainManager>,
    proof_challenge_repository: ProofChallengeRepository,
    triple_store_assertions: Arc<TripleStoreAssertions>,
    network_manager: Arc<NetworkManager>,
    assertion_validation: Arc<AssertionValidation>,
    peer_registry: Arc<PeerRegistry>,
}

impl ProvingTask {
    pub(crate) fn new(deps: ProvingDeps) -> Self {
        Self {
            blockchain_manager: deps.blockchain_manager,
            proof_challenge_repository: deps.proof_challenge_repository,
            triple_store_assertions: deps.triple_store_assertions,
            network_manager: deps.network_manager,
            assertion_validation: deps.assertion_validation,
            peer_registry: deps.peer_registry,
        }
    }

    /// Check if node is part of the shard for this blockchain.
    fn is_in_shard(&self, blockchain_id: &BlockchainId) -> bool {
        let peer_id = self.network_manager.peer_id();
        self.peer_registry.is_peer_in_shard(blockchain_id, peer_id)
    }

    /// Get the identity ID for this blockchain.
    fn identity_id(&self, blockchain_id: &BlockchainId) -> u128 {
        self.blockchain_manager.identity_id(blockchain_id)
    }

    /// Load shard peers for the given blockchain.
    fn load_shard_peers(&self, blockchain_id: &BlockchainId) -> Vec<PeerId> {
        let my_peer_id = self.network_manager.peer_id();
        self.peer_registry
            .select_shard_peers(blockchain_id, STREAM_PROTOCOL_GET, Some(my_peer_id))
    }

    /// Fetch assertion from network peers.
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
        let mut peers = self.load_shard_peers(&parsed_ual.blockchain);

        if peers.is_empty() {
            tracing::warn!("No peers available in shard");
            return None;
        }

        tracing::Span::current().record("peer_count", tracing::field::display(peers.len()));

        // Sort by latency (best performers first)
        self.peer_registry.sort_by_latency(&mut peers);

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

        if let Some(ack) = network_fetch::fetch_first_valid_ack_from_peers(
            Arc::clone(&self.network_manager),
            Arc::clone(&self.assertion_validation),
            peers,
            Uuid::new_v4(),
            request_data,
            parsed_ual.clone(),
            Visibility::Public,
        )
        .await
        {
            tracing::info!("Got valid assertion from network");
            return Some(Assertion::new(
                ack.assertion.public.clone(),
                ack.assertion.private.clone(),
            ));
        }

        tracing::warn!("Failed to fetch assertion from any peer");
        None
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("proving", shutdown, || self.execute(blockchain_id)).await;
    }

    #[tracing::instrument(name = "periodic_tasks.proving", skip(self,))]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let blockchain_label = blockchain_id.as_str();
        // 1. Check if we're in the shard
        if !self.is_in_shard(blockchain_id) {
            observability::record_proving_outcome(blockchain_label, "skipped_not_in_shard");
            tracing::debug!("Node not in shard, skipping proving");
            return PROVING_PERIOD;
        }

        let identity_id = self.identity_id(blockchain_id);

        // 2. Get active proof period status
        let proof_period_started = Instant::now();
        let proof_period = match self
            .blockchain_manager
            .get_active_proof_period_status(blockchain_id)
            .await
        {
            Ok(status) => {
                observability::record_proving_stage(
                    blockchain_label,
                    "proof_period_status",
                    "ok",
                    proof_period_started.elapsed(),
                );
                status
            }
            Err(e) => {
                observability::record_proving_stage(
                    blockchain_label,
                    "proof_period_status",
                    "error",
                    proof_period_started.elapsed(),
                );
                observability::record_proving_outcome(
                    blockchain_label,
                    "proof_period_status_error",
                );
                tracing::warn!(error = %e, "Failed to get proof period status");
                return PROVING_PERIOD;
            }
        };

        // 3. Get latest challenge from database
        let latest_challenge = self
            .proof_challenge_repository
            .get_latest(blockchain_id.as_str())
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
                let state = ChallengeState::from_db_value(&challenge.state);

                match state {
                    ChallengeState::Finalized => {
                        // Already finalized, nothing to do
                        observability::record_proving_outcome(
                            blockchain_label,
                            "already_finalized",
                        );
                        tracing::debug!("Challenge already finalized");
                        return PROVING_PERIOD;
                    }
                    ChallengeState::Submitted => {
                        // Check score on-chain first
                        let score = self
                            .blockchain_manager
                            .get_node_epoch_proof_period_score(
                                blockchain_id,
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
                                    observability::record_proving_outcome(
                                        blockchain_label,
                                        "waiting_reorg_buffer",
                                    );
                                    tracing::debug!(
                                        elapsed_secs = elapsed.as_secs(),
                                        buffer_secs = REORG_BUFFER.as_secs(),
                                        "Waiting for reorg buffer before finalizing"
                                    );
                                    return PROVING_PERIOD;
                                }

                                // Finalize
                                if let Err(e) = self
                                    .proof_challenge_repository
                                    .set_state(
                                        blockchain_id.as_str(),
                                        challenge.epoch,
                                        challenge.proof_period_start_block,
                                        ChallengeState::Finalized,
                                        Some(score.to_string()),
                                    )
                                    .await
                                {
                                    tracing::warn!(error = %e, "Failed to update challenge state");
                                }
                                observability::record_proving_outcome(
                                    blockchain_label,
                                    "finalized",
                                );
                                tracing::info!(score = %score, "Proof finalized successfully");
                                return PROVING_PERIOD;
                            }
                            Ok(_) => {
                                // Score is zero - reset to Pending and fall through to
                                // re-submit
                                tracing::warn!("Score is zero, resetting challenge to retry");
                                if let Err(e) = self
                                    .proof_challenge_repository
                                    .set_state(
                                        blockchain_id.as_str(),
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
                                observability::record_proving_outcome(
                                    blockchain_label,
                                    "score_lookup_error",
                                );
                                tracing::warn!(error = %e, "Failed to get proof score");
                                return PROVING_PERIOD;
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

        if needs_new_challenge {
            let create_challenge_started = Instant::now();
            if let Err(e) = self
                .blockchain_manager
                .create_challenge(blockchain_id)
                .await
            {
                observability::record_proving_stage(
                    blockchain_label,
                    "create_challenge",
                    "error",
                    create_challenge_started.elapsed(),
                );
                observability::record_proving_outcome(blockchain_label, "create_challenge_error");
                tracing::warn!(error = %e, "Failed to create challenge");
                return PROVING_PERIOD;
            }
            observability::record_proving_stage(
                blockchain_label,
                "create_challenge",
                "ok",
                create_challenge_started.elapsed(),
            );
        }

        // 5. Get challenge details from chain
        let node_challenge_started = Instant::now();
        let node_challenge = match self
            .blockchain_manager
            .get_node_challenge(blockchain_id, identity_id)
            .await
        {
            Ok(c) => {
                observability::record_proving_stage(
                    blockchain_label,
                    "load_challenge",
                    "ok",
                    node_challenge_started.elapsed(),
                );
                c
            }
            Err(e) => {
                observability::record_proving_stage(
                    blockchain_label,
                    "load_challenge",
                    "error",
                    node_challenge_started.elapsed(),
                );
                observability::record_proving_outcome(blockchain_label, "load_challenge_error");
                tracing::warn!(error = %e, "Failed to get node challenge");
                return PROVING_PERIOD;
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
            .proof_challenge_repository
            .get_latest(blockchain_id.as_str())
            .await
            .ok()
            .flatten();

        let needs_db_record = db_challenge
            .as_ref()
            .map(|c| c.proof_period_start_block != start_block)
            .unwrap_or(true);

        if needs_db_record
            && let Err(e) = self
                .proof_challenge_repository
                .create(
                    blockchain_id.as_str(),
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
            blockchain_id,
            &node_challenge.knowledge_collection_storage_contract,
            kc_id,
            None,
        );

        let parsed_ual = match dkg_domain::parse_ual(&ual) {
            Ok(u) => u,
            Err(e) => {
                observability::record_proving_outcome(blockchain_label, "parse_ual_error");
                tracing::warn!(error = %e, ual = %ual, "Failed to parse UAL");
                return PROVING_PERIOD;
            }
        };

        // Resolve token range with strict policy for blockchain errors.
        let token_ids_started = Instant::now();
        let token_ids = match resolve_token_ids(
            self.blockchain_manager.as_ref(),
            Uuid::new_v4(),
            &parsed_ual,
            TokenRangeResolutionPolicy::Strict,
        )
        .await
        {
            Ok(token_ids) => {
                observability::record_proving_stage(
                    blockchain_label,
                    "resolve_token_ids",
                    "ok",
                    token_ids_started.elapsed(),
                );
                token_ids
            }
            Err(e) => {
                observability::record_proving_stage(
                    blockchain_label,
                    "resolve_token_ids",
                    "error",
                    token_ids_started.elapsed(),
                );
                observability::record_proving_outcome(blockchain_label, "resolve_token_ids_error");
                tracing::warn!(error = %e, "Failed to resolve knowledge asset token range");
                return PROVING_PERIOD;
            }
        };

        // Try local triple store first, fall back to network.
        let local_fetch_started = Instant::now();
        let local_assertion = fetch_assertion_from_local(
            &self.triple_store_assertions,
            &self.assertion_validation,
            &parsed_ual,
            &token_ids,
            Visibility::Public,
        )
        .await;
        observability::record_proving_stage(
            blockchain_label,
            "fetch_local_assertion",
            if local_assertion.is_some() {
                "hit"
            } else {
                "miss"
            },
            local_fetch_started.elapsed(),
        );
        let assertion = match local_assertion {
            Some(local) => {
                tracing::debug!("Found valid assertion locally");
                local
            }
            None => {
                tracing::debug!(ual = %ual, "Assertion not found locally or invalid, trying network");
                let network_fetch_started = Instant::now();
                match self
                    .fetch_from_network(&parsed_ual, token_ids.clone())
                    .await
                {
                    Some(assertion) => {
                        observability::record_proving_stage(
                            blockchain_label,
                            "fetch_network_assertion",
                            "ok",
                            network_fetch_started.elapsed(),
                        );
                        assertion
                    }
                    None => {
                        observability::record_proving_stage(
                            blockchain_label,
                            "fetch_network_assertion",
                            "error",
                            network_fetch_started.elapsed(),
                        );
                        observability::record_proving_outcome(
                            blockchain_label,
                            "assertion_not_found",
                        );
                        tracing::warn!(ual = %ual, "Assertion not found on network");
                        return PROVING_PERIOD;
                    }
                }
            }
        };

        // 7. Calculate Merkle proof
        // Proving task receives triples already in single-triple-per-entry form from the
        // triple store / network ACK, so no normalization step is needed here (unlike
        // AssertionValidation which must handle multi-line network payloads).
        let merkle_started = Instant::now();
        let prepared_quads = match group_and_sort_public_triples(&assertion.public) {
            Ok(quads) => quads,
            Err(e) => {
                observability::record_proving_stage(
                    blockchain_label,
                    "build_merkle_proof",
                    "error",
                    merkle_started.elapsed(),
                );
                observability::record_proving_outcome(blockchain_label, "prepare_quads_error");
                tracing::warn!(error = %e, "Failed to prepare quads for proof");
                return PROVING_PERIOD;
            }
        };

        let proof_result = match dkg_domain::calculate_merkle_proof(&prepared_quads, chunk_index) {
            Ok(result) => result,
            Err(e) => {
                observability::record_proving_stage(
                    blockchain_label,
                    "build_merkle_proof",
                    "error",
                    merkle_started.elapsed(),
                );
                observability::record_proving_outcome(
                    blockchain_label,
                    "calculate_merkle_proof_error",
                );
                tracing::warn!(error = %e, chunk_index, "Failed to calculate Merkle proof");
                return PROVING_PERIOD;
            }
        };
        observability::record_proving_stage(
            blockchain_label,
            "build_merkle_proof",
            "ok",
            merkle_started.elapsed(),
        );

        // Convert B256 to [u8; 32]
        let proof_bytes: Vec<[u8; 32]> = proof_result.proof.iter().map(|b| *b.as_ref()).collect();

        // 8. Submit proof
        let submit_started = Instant::now();
        if let Err(e) = self
            .blockchain_manager
            .submit_proof(blockchain_id, &proof_result.chunk, &proof_bytes)
            .await
        {
            observability::record_proving_stage(
                blockchain_label,
                "submit_proof",
                "error",
                submit_started.elapsed(),
            );
            observability::record_proving_outcome(blockchain_label, "submit_proof_error");
            tracing::warn!(error = %e, "Failed to submit proof");
            return PROVING_PERIOD;
        }
        observability::record_proving_stage(
            blockchain_label,
            "submit_proof",
            "ok",
            submit_started.elapsed(),
        );

        // 9. Update database state
        if let Err(e) = self
            .proof_challenge_repository
            .set_state(
                blockchain_id.as_str(),
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
        observability::record_proving_outcome(blockchain_label, "submitted");

        PROVING_PERIOD
    }
}
