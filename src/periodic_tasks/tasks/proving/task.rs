//! Proving periodic task implementation.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use dkg_blockchain::{BlockchainManager, U256};
use dkg_domain::{BlockchainId, Visibility, derive_ual};
use dkg_network::STREAM_PROTOCOL_GET;
use dkg_repository::{ChallengeState, ProofChallengeRepository};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{PROVING_PERIOD, REORG_BUFFER};
use crate::{
    application::{
        AssertionRetrieval, FetchRequest, ShardPeerSelection, TokenRangeResolutionPolicy,
        group_and_sort_public_triples,
    },
    periodic_tasks::ProvingDeps,
    periodic_tasks::runner::run_with_shutdown,
};

pub(crate) struct ProvingTask {
    blockchain_manager: Arc<BlockchainManager>,
    proof_challenge_repository: ProofChallengeRepository,
    assertion_retrieval: Arc<AssertionRetrieval>,
    shard_peer_selection: Arc<ShardPeerSelection>,
}

impl ProvingTask {
    pub(crate) fn new(deps: ProvingDeps) -> Self {
        Self {
            blockchain_manager: deps.blockchain_manager,
            proof_challenge_repository: deps.proof_challenge_repository,
            assertion_retrieval: deps.assertion_retrieval,
            shard_peer_selection: deps.shard_peer_selection,
        }
    }

    /// Get the identity ID for this blockchain.
    fn identity_id(&self, blockchain_id: &BlockchainId) -> u128 {
        self.blockchain_manager.identity_id(blockchain_id)
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("proving", shutdown, || self.execute(blockchain_id)).await;
    }

    #[tracing::instrument(name = "periodic_tasks.proving", skip(self,))]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        // 1. Check if we're in the shard
        if !self
            .shard_peer_selection
            .is_local_node_in_shard(blockchain_id)
        {
            tracing::debug!("Node not in shard, skipping proving");
            return PROVING_PERIOD;
        }

        let identity_id = self.identity_id(blockchain_id);

        // 2. Get active proof period status
        let proof_period = match self
            .blockchain_manager
            .get_active_proof_period_status(blockchain_id)
            .await
        {
            Ok(status) => status,
            Err(e) => {
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

        if needs_new_challenge
            && let Err(e) = self
                .blockchain_manager
                .create_challenge(blockchain_id)
                .await
        {
            tracing::warn!(error = %e, "Failed to create challenge");
            return PROVING_PERIOD;
        }

        // 5. Get challenge details from chain
        let node_challenge = match self
            .blockchain_manager
            .get_node_challenge(blockchain_id, identity_id)
            .await
        {
            Ok(c) => c,
            Err(e) => {
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
                tracing::warn!(error = %e, ual = %ual, "Failed to parse UAL");
                return PROVING_PERIOD;
            }
        };

        let operation_id = Uuid::new_v4();

        // Resolve token range with strict policy for blockchain errors.
        let token_ids = match self
            .assertion_retrieval
            .resolve_token_ids(
                operation_id,
                &parsed_ual,
                TokenRangeResolutionPolicy::Strict,
            )
            .await
        {
            Ok(token_ids) => token_ids,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to resolve knowledge asset token range");
                return PROVING_PERIOD;
            }
        };

        let peers = self
            .shard_peer_selection
            .load_shard_peers(&parsed_ual.blockchain, STREAM_PROTOCOL_GET);

        let fetch_request = FetchRequest {
            operation_id,
            parsed_ual: parsed_ual.clone(),
            token_ids: token_ids.clone(),
            peers,
            visibility: Visibility::Public,
            include_metadata: false,
            paranet_ual: None,
        };

        let assertion = match self.assertion_retrieval.fetch(&fetch_request).await {
            Ok(fetched) => {
                tracing::debug!(source = ?fetched.source, "Fetched assertion for proving");
                fetched.assertion
            }
            Err(e) => {
                tracing::warn!(error = %e, ual = %ual, "Failed to fetch assertion");
                return PROVING_PERIOD;
            }
        };

        // 7. Calculate Merkle proof
        // Proving task receives triples already in single-triple-per-entry form from the
        // triple store / network ACK, so no normalization step is needed here (unlike
        // AssertionValidation which must handle multi-line network payloads).
        let prepared_quads = match group_and_sort_public_triples(&assertion.public) {
            Ok(quads) => quads,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to prepare quads for proof");
                return PROVING_PERIOD;
            }
        };

        let proof_result = match dkg_domain::calculate_merkle_proof(&prepared_quads, chunk_index) {
            Ok(result) => result,
            Err(e) => {
                tracing::warn!(error = %e, chunk_index, "Failed to calculate Merkle proof");
                return PROVING_PERIOD;
            }
        };

        // Convert B256 to [u8; 32]
        let proof_bytes: Vec<[u8; 32]> = proof_result.proof.iter().map(|b| *b.as_ref()).collect();

        // 8. Submit proof
        if let Err(e) = self
            .blockchain_manager
            .submit_proof(blockchain_id, &proof_result.chunk, &proof_bytes)
            .await
        {
            tracing::warn!(error = %e, "Failed to submit proof");
            return PROVING_PERIOD;
        }

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

        PROVING_PERIOD
    }
}
