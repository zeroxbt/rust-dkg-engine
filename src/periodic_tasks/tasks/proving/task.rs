//! Proving periodic task implementation.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use dkg_blockchain::{BlockchainManager, U256};
use dkg_domain::{Assertion, BlockchainId, ParsedUal, Visibility, derive_ual};
use dkg_network::{NetworkManager, STREAM_PROTOCOL_GET};
use dkg_peer_registry::PeerRegistry;
use dkg_repository::{ChallengeState, ProofChallengeRepository};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{PROVING_PERIOD, REORG_BUFFER};
use crate::{
    application::{
        AssertionRetrieval, FetchRequest, TokenRangeResolutionPolicy, group_and_sort_public_triples,
    },
    periodic_tasks::ProvingDeps,
    periodic_tasks::runner::run_with_shutdown,
};

pub(crate) struct ProvingTask {
    blockchain_manager: Arc<BlockchainManager>,
    network_manager: Arc<NetworkManager>,
    peer_registry: Arc<PeerRegistry>,
    proof_challenge_repository: ProofChallengeRepository,
    assertion_retrieval: Arc<AssertionRetrieval>,
}

#[derive(Clone)]
struct LatestChallengeSnapshot {
    epoch: i64,
    proof_period_start_block: i64,
    state: ChallengeState,
    updated_at: i64,
}

struct ProofWorkItem {
    epoch: i64,
    start_block: i64,
    kc_id: u128,
    chunk_index: usize,
    contract_addr: String,
    ual: String,
    parsed_ual: ParsedUal,
}

fn should_create_new_challenge(
    proof_period_is_valid: bool,
    current_start_block: U256,
    latest_challenge: Option<&LatestChallengeSnapshot>,
) -> bool {
    if !proof_period_is_valid {
        return true;
    }

    latest_challenge
        .map(|c| U256::from(c.proof_period_start_block as u64) != current_start_block)
        .unwrap_or(true)
}

fn elapsed_since_timestamp_secs(updated_at: i64, now: i64) -> Duration {
    Duration::from_secs((now - updated_at).max(0) as u64)
}

fn should_wait_for_reorg_finalization(updated_at: i64, now: i64) -> bool {
    elapsed_since_timestamp_secs(updated_at, now) < REORG_BUFFER
}

impl ProvingTask {
    pub(crate) fn new(deps: ProvingDeps) -> Self {
        Self {
            blockchain_manager: deps.blockchain_manager,
            network_manager: deps.network_manager,
            peer_registry: deps.peer_registry,
            proof_challenge_repository: deps.proof_challenge_repository,
            assertion_retrieval: deps.assertion_retrieval,
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
        let local_peer_id = self.network_manager.peer_id();
        if !self
            .peer_registry
            .is_peer_in_shard(blockchain_id, local_peer_id)
        {
            tracing::debug!("Node not in shard, skipping proving");
            return PROVING_PERIOD;
        }

        let identity_id = self.identity_id(blockchain_id);

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
        let latest_challenge = self.load_latest_challenge_snapshot(blockchain_id).await;

        let current_start_block = proof_period.active_proof_period_start_block;

        if !proof_period.is_valid {
            tracing::debug!(
                active_start_block = %proof_period.active_proof_period_start_block,
                "No valid proof period active"
            );
        }

        if !self
            .continue_after_existing_challenge_check(
                blockchain_id,
                identity_id,
                proof_period.is_valid,
                current_start_block,
                latest_challenge.as_ref(),
            )
            .await
        {
            return PROVING_PERIOD;
        }

        if self.needs_new_challenge(
            proof_period.is_valid,
            current_start_block,
            latest_challenge.as_ref(),
        ) && !self.maybe_create_challenge(blockchain_id).await
        {
            return PROVING_PERIOD;
        }

        let Some(work_item) = self.load_proof_work_item(blockchain_id, identity_id).await else {
            return PROVING_PERIOD;
        };
        self.ensure_challenge_record(blockchain_id, &work_item)
            .await;

        let Some(assertion) = self.fetch_assertion_for_proving(&work_item).await else {
            return PROVING_PERIOD;
        };
        let _ = self
            .submit_proof_and_mark_state(blockchain_id, &work_item, &assertion)
            .await;

        PROVING_PERIOD
    }

    async fn load_latest_challenge_snapshot(
        &self,
        blockchain_id: &BlockchainId,
    ) -> Option<LatestChallengeSnapshot> {
        let challenge = self
            .proof_challenge_repository
            .get_latest(blockchain_id.as_str())
            .await
            .ok()
            .flatten()?;

        Some(LatestChallengeSnapshot {
            epoch: challenge.epoch,
            proof_period_start_block: challenge.proof_period_start_block,
            state: ChallengeState::from_db_value(&challenge.state),
            updated_at: challenge.updated_at,
        })
    }

    async fn continue_after_existing_challenge_check(
        &self,
        blockchain_id: &BlockchainId,
        identity_id: u128,
        proof_period_is_valid: bool,
        current_start_block: U256,
        latest_challenge: Option<&LatestChallengeSnapshot>,
    ) -> bool {
        if !proof_period_is_valid {
            return true;
        }

        let Some(challenge) = latest_challenge else {
            return true;
        };

        let challenge_start_block = U256::from(challenge.proof_period_start_block as u64);
        if challenge_start_block != current_start_block {
            return true;
        }

        match challenge.state {
            ChallengeState::Finalized => {
                tracing::debug!("Challenge already finalized");
                false
            }
            ChallengeState::Pending => true,
            ChallengeState::Submitted => {
                self.handle_submitted_challenge_state(
                    blockchain_id,
                    identity_id,
                    challenge,
                    challenge_start_block,
                )
                .await
            }
        }
    }

    async fn handle_submitted_challenge_state(
        &self,
        blockchain_id: &BlockchainId,
        identity_id: u128,
        challenge: &LatestChallengeSnapshot,
        challenge_start_block: U256,
    ) -> bool {
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
                let now = Utc::now().timestamp();
                let elapsed = elapsed_since_timestamp_secs(challenge.updated_at, now);
                if should_wait_for_reorg_finalization(challenge.updated_at, now) {
                    tracing::debug!(
                        elapsed_secs = elapsed.as_secs(),
                        buffer_secs = REORG_BUFFER.as_secs(),
                        "Waiting for reorg buffer before finalizing"
                    );
                    return false;
                }

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
                false
            }
            Ok(_) => {
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
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get proof score");
                false
            }
        }
    }

    fn needs_new_challenge(
        &self,
        proof_period_is_valid: bool,
        current_start_block: U256,
        latest_challenge: Option<&LatestChallengeSnapshot>,
    ) -> bool {
        should_create_new_challenge(proof_period_is_valid, current_start_block, latest_challenge)
    }

    async fn maybe_create_challenge(&self, blockchain_id: &BlockchainId) -> bool {
        if let Err(e) = self
            .blockchain_manager
            .create_challenge(blockchain_id)
            .await
        {
            tracing::warn!(error = %e, "Failed to create challenge");
            return false;
        }
        true
    }

    async fn load_proof_work_item(
        &self,
        blockchain_id: &BlockchainId,
        identity_id: u128,
    ) -> Option<ProofWorkItem> {
        let node_challenge = match self
            .blockchain_manager
            .get_node_challenge(blockchain_id, identity_id)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get node challenge");
                return None;
            }
        };

        let epoch = node_challenge.epoch.as_limbs()[0] as i64;
        let start_block = node_challenge.active_proof_period_start_block.as_limbs()[0] as i64;
        let kc_id = node_challenge.knowledge_collection_id.as_limbs()[0] as u128;
        let chunk_index = node_challenge.chunk_id.as_limbs()[0] as usize;
        let contract_addr =
            format!("{:?}", node_challenge.knowledge_collection_storage_contract).to_lowercase();
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
                return None;
            }
        };

        Some(ProofWorkItem {
            epoch,
            start_block,
            kc_id,
            chunk_index,
            contract_addr,
            ual,
            parsed_ual,
        })
    }

    async fn ensure_challenge_record(
        &self,
        blockchain_id: &BlockchainId,
        work_item: &ProofWorkItem,
    ) {
        let latest = self.load_latest_challenge_snapshot(blockchain_id).await;
        let needs_db_record = latest
            .as_ref()
            .map(|c| c.proof_period_start_block != work_item.start_block)
            .unwrap_or(true);

        if !needs_db_record {
            return;
        }

        if let Err(e) = self
            .proof_challenge_repository
            .create(
                blockchain_id.as_str(),
                work_item.epoch,
                work_item.start_block,
                &work_item.contract_addr,
                work_item.kc_id as i64,
                work_item.chunk_index as i64,
            )
            .await
        {
            tracing::warn!(error = %e, "Failed to store challenge");
        }
    }

    async fn fetch_assertion_for_proving(&self, work_item: &ProofWorkItem) -> Option<Assertion> {
        let operation_id = Uuid::new_v4();
        let token_ids = match self
            .assertion_retrieval
            .resolve_token_ids(
                operation_id,
                &work_item.parsed_ual,
                TokenRangeResolutionPolicy::Strict,
            )
            .await
        {
            Ok(token_ids) => token_ids,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to resolve knowledge asset token range");
                return None;
            }
        };

        let peers = self.peer_registry.select_shard_peers(
            &work_item.parsed_ual.blockchain,
            STREAM_PROTOCOL_GET,
            Some(self.network_manager.peer_id()),
        );

        let fetch_request = FetchRequest {
            operation_id,
            parsed_ual: work_item.parsed_ual.clone(),
            token_ids,
            peers,
            visibility: Visibility::Public,
            include_metadata: false,
            paranet_ual: None,
        };

        match self.assertion_retrieval.fetch(&fetch_request).await {
            Ok(fetched) => {
                tracing::debug!(source = ?fetched.source, "Fetched assertion for proving");
                Some(fetched.assertion)
            }
            Err(e) => {
                tracing::warn!(error = %e, ual = %work_item.ual, "Failed to fetch assertion");
                None
            }
        }
    }

    async fn submit_proof_and_mark_state(
        &self,
        blockchain_id: &BlockchainId,
        work_item: &ProofWorkItem,
        assertion: &Assertion,
    ) -> bool {
        // Proving task receives triples already in single-triple-per-entry form from the
        // triple store / network ACK, so no normalization step is needed here (unlike
        // AssertionValidation which must handle multi-line network payloads).
        let prepared_quads = match group_and_sort_public_triples(&assertion.public) {
            Ok(quads) => quads,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to prepare quads for proof");
                return false;
            }
        };

        let proof_result =
            match dkg_domain::calculate_merkle_proof(&prepared_quads, work_item.chunk_index) {
                Ok(result) => result,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        chunk_index = work_item.chunk_index,
                        "Failed to calculate Merkle proof"
                    );
                    return false;
                }
            };

        let proof_bytes: Vec<[u8; 32]> = proof_result.proof.iter().map(|b| *b.as_ref()).collect();
        if let Err(e) = self
            .blockchain_manager
            .submit_proof(blockchain_id, &proof_result.chunk, &proof_bytes)
            .await
        {
            tracing::warn!(error = %e, "Failed to submit proof");
            return false;
        }

        if let Err(e) = self
            .proof_challenge_repository
            .set_state(
                blockchain_id.as_str(),
                work_item.epoch,
                work_item.start_block,
                ChallengeState::Submitted,
                None,
            )
            .await
        {
            tracing::warn!(error = %e, "Failed to update challenge state");
        }

        tracing::info!(
            epoch = work_item.epoch,
            chunk_index = work_item.chunk_index,
            "Proof submitted successfully"
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn challenge(start_block: i64) -> LatestChallengeSnapshot {
        LatestChallengeSnapshot {
            epoch: 1,
            proof_period_start_block: start_block,
            state: ChallengeState::Pending,
            updated_at: 0,
        }
    }

    #[test]
    fn create_new_challenge_when_period_invalid() {
        let current_start_block = U256::from(100u64);
        assert!(should_create_new_challenge(
            false,
            current_start_block,
            Some(&challenge(100))
        ));
    }

    #[test]
    fn create_new_challenge_when_no_previous_challenge() {
        let current_start_block = U256::from(100u64);
        assert!(should_create_new_challenge(true, current_start_block, None));
    }

    #[test]
    fn do_not_create_new_challenge_when_start_block_matches() {
        let current_start_block = U256::from(100u64);
        assert!(!should_create_new_challenge(
            true,
            current_start_block,
            Some(&challenge(100))
        ));
    }

    #[test]
    fn create_new_challenge_when_start_block_differs() {
        let current_start_block = U256::from(200u64);
        assert!(should_create_new_challenge(
            true,
            current_start_block,
            Some(&challenge(100))
        ));
    }

    #[test]
    fn wait_for_reorg_when_elapsed_below_buffer() {
        let updated_at = 1_000;
        let now = updated_at + REORG_BUFFER.as_secs() as i64 - 1;
        assert!(should_wait_for_reorg_finalization(updated_at, now));
    }

    #[test]
    fn do_not_wait_for_reorg_when_elapsed_at_or_above_buffer() {
        let updated_at = 1_000;
        let now = updated_at + REORG_BUFFER.as_secs() as i64;
        assert!(!should_wait_for_reorg_finalization(updated_at, now));
    }

    #[test]
    fn elapsed_since_timestamp_clamps_negative_durations() {
        let updated_at = 2_000;
        let now = 1_000;
        assert_eq!(
            elapsed_since_timestamp_secs(updated_at, now),
            Duration::ZERO
        );
        assert!(should_wait_for_reorg_finalization(updated_at, now));
    }
}
