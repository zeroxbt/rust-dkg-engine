use std::collections::HashMap;

use dkg_blockchain::BlockchainId;
use dkg_repository::{KcChainMetadataRepository, KcProjectionRepository, error::RepositoryError};

pub(crate) struct HydrateProjectionOutcome {
    pub(crate) missing_keys: usize,
    pub(crate) hydrated_keys: usize,
    pub(crate) failed_keys: usize,
    pub(crate) failed_contracts: usize,
}

pub(crate) async fn run(
    blockchain_id: &BlockchainId,
    batch_size: usize,
    kc_chain_metadata_repository: &KcChainMetadataRepository,
    kc_projection_repository: &KcProjectionRepository,
) -> Result<HydrateProjectionOutcome, RepositoryError> {
    let missing_projection_keys = kc_chain_metadata_repository
        .list_ready_keys_missing_projection(blockchain_id.as_str(), batch_size.max(1))
        .await?;

    if missing_projection_keys.is_empty() {
        return Ok(HydrateProjectionOutcome {
            missing_keys: 0,
            hydrated_keys: 0,
            failed_keys: 0,
            failed_contracts: 0,
        });
    }

    let mut by_contract: HashMap<String, Vec<u64>> = HashMap::new();
    for (contract_address, kc_id) in &missing_projection_keys {
        by_contract
            .entry(contract_address.clone())
            .or_default()
            .push(*kc_id);
    }

    let mut hydrated_keys = 0usize;
    let mut failed_keys = 0usize;
    let mut failed_contracts = 0usize;

    for (contract_address, mut kc_ids) in by_contract {
        kc_ids.sort_unstable();
        kc_ids.dedup();
        let count = kc_ids.len();

        match kc_projection_repository
            .ensure_desired_present(blockchain_id.as_str(), &contract_address, &kc_ids)
            .await
        {
            Ok(()) => {
                hydrated_keys = hydrated_keys.saturating_add(count);
            }
            Err(error) => {
                failed_contracts = failed_contracts.saturating_add(1);
                failed_keys = failed_keys.saturating_add(count);
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    contract_address = %contract_address,
                    count,
                    error = %error,
                    "KC reconciliation hydration failed for contract"
                );
            }
        }
    }

    Ok(HydrateProjectionOutcome {
        missing_keys: missing_projection_keys.len(),
        hydrated_keys,
        failed_keys,
        failed_contracts,
    })
}
