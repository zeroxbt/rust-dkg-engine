use std::collections::HashMap;

use dkg_blockchain::{Address, BlockchainId};
use dkg_domain::derive_ual;
use dkg_repository::{
    KcChainMetadataRepository, KcProjectionRepository, KcSyncRepository, error::RepositoryError,
};
use dkg_triple_store::error::TripleStoreError;

use crate::application::TripleStoreAssertions;

const REASON_METADATA_MISSING: &str = "reconcile_metadata_missing";
const REASON_INVALID_CONTRACT_ADDRESS: &str = "reconcile_invalid_contract_address";
const REASON_MISSING_IN_TRIPLE_STORE: &str = "reconcile_missing_in_triplestore";

pub(crate) struct ReconcileNonPresentOutcome {
    pub(crate) candidates: usize,
    pub(crate) found_present: usize,
    pub(crate) enqueued: usize,
    pub(crate) metadata_missing: usize,
    pub(crate) failed_projection_updates: usize,
}

#[derive(Debug)]
pub(crate) enum ReconcileNonPresentError {
    Repository(RepositoryError),
    TripleStore(TripleStoreError),
}

impl std::fmt::Display for ReconcileNonPresentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Repository(error) => write!(f, "{error}"),
            Self::TripleStore(error) => write!(f, "{error}"),
        }
    }
}

impl From<RepositoryError> for ReconcileNonPresentError {
    fn from(value: RepositoryError) -> Self {
        Self::Repository(value)
    }
}

impl From<TripleStoreError> for ReconcileNonPresentError {
    fn from(value: TripleStoreError) -> Self {
        Self::TripleStore(value)
    }
}

struct PreparedCandidate {
    contract_address: String,
    kc_id: u64,
    ual: String,
}

pub(crate) async fn run(
    blockchain_id: &BlockchainId,
    batch_size: usize,
    kc_projection_repository: &KcProjectionRepository,
    kc_chain_metadata_repository: &KcChainMetadataRepository,
    kc_sync_repository: &KcSyncRepository,
    triple_store_assertions: &TripleStoreAssertions,
) -> Result<ReconcileNonPresentOutcome, ReconcileNonPresentError> {
    let candidates = kc_projection_repository
        .list_non_present_desired_keys(blockchain_id.as_str(), batch_size.max(1))
        .await?;
    let candidates_count = candidates.len();
    if candidates.is_empty() {
        return Ok(ReconcileNonPresentOutcome {
            candidates: 0,
            found_present: 0,
            enqueued: 0,
            metadata_missing: 0,
            failed_projection_updates: 0,
        });
    }

    let mut prepared = Vec::with_capacity(candidates.len());
    let mut uals = Vec::with_capacity(candidates.len());
    let mut invalid_contract_rows: HashMap<String, Vec<u64>> = HashMap::new();
    for (contract_address, kc_id) in candidates {
        let Ok(contract) = contract_address.parse::<Address>() else {
            invalid_contract_rows
                .entry(contract_address)
                .or_default()
                .push(kc_id);
            continue;
        };
        let ual = derive_ual(blockchain_id, &contract, u128::from(kc_id), None);
        uals.push(ual.clone());
        prepared.push(PreparedCandidate {
            contract_address,
            kc_id,
            ual,
        });
    }

    let existing = triple_store_assertions
        .try_knowledge_collections_exist_by_uals(&uals)
        .await?;

    let mut present_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    let mut missing_candidates: Vec<(String, u64)> = Vec::new();

    for PreparedCandidate {
        contract_address,
        kc_id,
        ual,
    } in prepared
    {
        if existing.contains(&ual) {
            present_by_contract
                .entry(contract_address)
                .or_default()
                .push(kc_id);
        } else {
            missing_candidates.push((contract_address, kc_id));
        }
    }

    let found_present: usize = present_by_contract.values().map(Vec::len).sum();
    for (contract_address, mut kc_ids) in present_by_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        kc_projection_repository
            .mark_present(blockchain_id.as_str(), &contract_address, &kc_ids)
            .await?;
        kc_sync_repository
            .remove_kcs(blockchain_id.as_str(), &contract_address, &kc_ids)
            .await?;
    }

    let missing_keys: Vec<(String, u64)> = missing_candidates
        .iter()
        .map(|(contract, kc_id)| (contract.clone(), *kc_id))
        .collect();
    let ready_by_key = kc_chain_metadata_repository
        .get_many_ready_with_kc_state_metadata_for_keys(blockchain_id.as_str(), &missing_keys)
        .await?;

    let mut enqueue_by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    let mut mark_missing_by_contract: HashMap<String, Vec<u64>> = HashMap::new();

    for (contract_address, kc_id) in missing_candidates {
        let key = (contract_address.clone(), kc_id);
        if ready_by_key.contains_key(&key) {
            enqueue_by_contract
                .entry(contract_address.clone())
                .or_default()
                .push(kc_id);
        } else {
            mark_missing_by_contract
                .entry(contract_address)
                .or_default()
                .push(kc_id);
        }
    }

    let mut enqueued = 0usize;
    let mut failed_projection_updates = 0usize;
    for (contract_address, mut kc_ids) in enqueue_by_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        let count = kc_ids.len();
        kc_sync_repository
            .enqueue_kcs(blockchain_id.as_str(), &contract_address, &kc_ids)
            .await?;
        if let Err(error) = kc_projection_repository
            .mark_pending_with_error(
                blockchain_id.as_str(),
                &contract_address,
                &kc_ids,
                Some(REASON_MISSING_IN_TRIPLE_STORE),
            )
            .await
        {
            failed_projection_updates = failed_projection_updates.saturating_add(kc_ids.len());
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract_address = %contract_address,
                count = kc_ids.len(),
                error = %error,
                "KC reconciliation failed to mark enqueued projection rows as pending"
            );
        }
        enqueued = enqueued.saturating_add(count);
    }

    let mut metadata_missing = 0usize;
    for (contract_address, mut kc_ids) in mark_missing_by_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        metadata_missing = metadata_missing.saturating_add(kc_ids.len());
        if let Err(error) = kc_projection_repository
            .mark_failed(
                blockchain_id.as_str(),
                &contract_address,
                &kc_ids,
                REASON_METADATA_MISSING,
            )
            .await
        {
            failed_projection_updates = failed_projection_updates.saturating_add(kc_ids.len());
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract_address = %contract_address,
                count = kc_ids.len(),
                error = %error,
                "KC reconciliation failed to mark metadata-missing projection rows"
            );
        }
    }

    for (contract_address, mut kc_ids) in invalid_contract_rows {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        tracing::warn!(
            blockchain_id = %blockchain_id,
            contract_address = %contract_address,
            count = kc_ids.len(),
            "KC reconciliation found projection rows with invalid contract address"
        );
        if let Err(error) = kc_projection_repository
            .mark_failed(
                blockchain_id.as_str(),
                &contract_address,
                &kc_ids,
                REASON_INVALID_CONTRACT_ADDRESS,
            )
            .await
        {
            failed_projection_updates = failed_projection_updates.saturating_add(kc_ids.len());
            tracing::warn!(
                blockchain_id = %blockchain_id,
                contract_address = %contract_address,
                count = kc_ids.len(),
                error = %error,
                "KC reconciliation failed to mark invalid-contract projection rows"
            );
        }
    }

    Ok(ReconcileNonPresentOutcome {
        candidates: candidates_count,
        found_present,
        enqueued,
        metadata_missing,
        failed_projection_updates,
    })
}
