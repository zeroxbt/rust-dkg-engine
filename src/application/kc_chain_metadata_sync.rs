use std::collections::HashMap;

use dkg_blockchain::{
    Address, B256, BlockchainId, BlockchainManager, MulticallBatch, MulticallRequest, U256,
    encoders, to_hex_string,
};
use dkg_domain::TokenIds;
use dkg_repository::{KcChainMetadataRepository, error::RepositoryError};
use futures::{StreamExt, stream};

use crate::application::state_metadata::encode_burned_ids;

const BLOCK_TIMESTAMP_FETCH_CONCURRENCY: usize = 16;

#[derive(Debug, Clone)]
pub(crate) struct KcHydratedStateMetadata {
    pub(crate) range_start_token_id: u64,
    pub(crate) range_end_token_id: u64,
    pub(crate) burned_mode: u32,
    pub(crate) burned_payload: Vec<u8>,
    pub(crate) end_epoch: u64,
    pub(crate) latest_merkle_root: String,
}

#[derive(Debug, Clone)]
pub(crate) struct KcChainMetadataRecord {
    pub(crate) publish_operation_id: String,
    pub(crate) kc_id: u64,
    pub(crate) merkle_root: B256,
    pub(crate) byte_size: u128,
    pub(crate) contract_address: Address,
    pub(crate) transaction_hash: B256,
    pub(crate) block_number: u64,
    pub(crate) block_timestamp: u64,
    pub(crate) publisher_address: Option<String>,
    pub(crate) kc_state_metadata: Option<KcHydratedStateMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuildKcRecordError {
    KcIdOutOfRange,
    MissingTransactionHash,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_kc_chain_metadata_record(
    publish_operation_id: String,
    knowledge_collection_id: U256,
    merkle_root: B256,
    byte_size: u128,
    contract_address: Address,
    transaction_hash: Option<B256>,
    block_number: u64,
    block_timestamp: u64,
) -> Result<KcChainMetadataRecord, BuildKcRecordError> {
    let kc_id_u128: u128 = knowledge_collection_id.to();
    let kc_id = u64::try_from(kc_id_u128).map_err(|_| BuildKcRecordError::KcIdOutOfRange)?;
    let transaction_hash = transaction_hash.ok_or(BuildKcRecordError::MissingTransactionHash)?;

    Ok(KcChainMetadataRecord {
        publish_operation_id,
        kc_id,
        merkle_root,
        byte_size,
        contract_address,
        transaction_hash,
        block_number,
        block_timestamp,
        publisher_address: None,
        kc_state_metadata: None,
    })
}

pub(crate) async fn hydrate_core_metadata_publishers(
    blockchain_manager: &BlockchainManager,
    blockchain_id: &BlockchainId,
    records: &mut [KcChainMetadataRecord],
) {
    if records.is_empty() {
        return;
    }

    for record in records.iter_mut() {
        record.publisher_address = match blockchain_manager
            .get_transaction_sender(blockchain_id, record.transaction_hash)
            .await
        {
            Ok(Some(address)) => Some(format!("{:?}", address)),
            _ => None,
        };
    }
}

pub(crate) async fn hydrate_block_timestamps(
    blockchain_manager: &BlockchainManager,
    blockchain_id: &BlockchainId,
    records: &mut [KcChainMetadataRecord],
) {
    if records.is_empty() {
        return;
    }

    let mut timestamps_by_block: HashMap<u64, u64> = HashMap::new();
    let mut blocks: Vec<u64> = records
        .iter()
        .filter(|record| record.block_timestamp == 0)
        .map(|record| record.block_number)
        .collect();
    blocks.sort_unstable();
    blocks.dedup();

    let block_results = stream::iter(blocks.into_iter())
        .map(|block_number| async move {
            (
                block_number,
                blockchain_manager
                    .get_block_timestamp(blockchain_id, block_number)
                    .await,
            )
        })
        .buffer_unordered(BLOCK_TIMESTAMP_FETCH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    for (block_number, result) in block_results {
        match result {
            Ok(Some(timestamp)) => {
                timestamps_by_block.insert(block_number, timestamp);
            }
            Ok(None) => {
                tracing::warn!(
                    blockchain = %blockchain_id,
                    block_number,
                    "Missing block while hydrating KC metadata timestamp"
                );
            }
            Err(error) => {
                tracing::warn!(
                    blockchain = %blockchain_id,
                    block_number,
                    error = %error,
                    "Failed to hydrate KC metadata timestamp"
                );
            }
        }
    }

    for record in records.iter_mut() {
        if record.block_timestamp == 0
            && let Some(timestamp) = timestamps_by_block.get(&record.block_number).copied()
        {
            record.block_timestamp = timestamp;
        }
    }
}

pub(crate) async fn hydrate_kc_state_metadata(
    blockchain_manager: &BlockchainManager,
    blockchain_id: &BlockchainId,
    records: &mut [KcChainMetadataRecord],
    chunk_size: usize,
) {
    if records.is_empty() {
        return;
    }

    for chunk in records.chunks_mut(chunk_size.max(1)) {
        let mut state_calls = MulticallBatch::with_capacity(chunk.len() * 3);
        for record in chunk.iter() {
            state_calls.add(MulticallRequest::new(
                record.contract_address,
                encoders::encode_get_end_epoch(record.kc_id as u128),
            ));
            state_calls.add(MulticallRequest::new(
                record.contract_address,
                encoders::encode_get_knowledge_assets_range(record.kc_id as u128),
            ));
            state_calls.add(MulticallRequest::new(
                record.contract_address,
                encoders::encode_get_merkle_root(record.kc_id as u128),
            ));
        }

        match blockchain_manager
            .execute_multicall(blockchain_id, state_calls)
            .await
        {
            Ok(results) => {
                let expected = chunk.len() * 3;
                if results.len() != expected {
                    tracing::warn!(
                        blockchain = %blockchain_id,
                        expected_results = expected,
                        actual_results = results.len(),
                        batch_size = chunk.len(),
                        "Unexpected multicall result count for KC state metadata hydration batch"
                    );
                    continue;
                }

                for (index, record) in chunk.iter_mut().enumerate() {
                    let base = index * 3;
                    let epoch_result = &results[base];
                    let range_result = &results[base + 1];
                    let merkle_result = &results[base + 2];

                    let Some((global_start, global_end, global_burned)) =
                        range_result.as_knowledge_assets_range()
                    else {
                        tracing::warn!(
                            blockchain = %blockchain_id,
                            contract = ?record.contract_address,
                            kc_id = record.kc_id,
                            "Failed to decode KC token range during state metadata hydration"
                        );
                        continue;
                    };

                    let Some(end_epoch) = epoch_result.as_u64().filter(|v| *v != 0) else {
                        tracing::warn!(
                            blockchain = %blockchain_id,
                            contract = ?record.contract_address,
                            kc_id = record.kc_id,
                            "Missing/zero end_epoch during state metadata hydration"
                        );
                        continue;
                    };

                    if record.kc_id == 0 {
                        tracing::warn!(
                            blockchain = %blockchain_id,
                            contract = ?record.contract_address,
                            kc_id = record.kc_id,
                            "Invalid KC id for token range normalization"
                        );
                        continue;
                    }

                    // Chain returns global token ids; normalize to per-KC local token ids
                    // expected by triple-store UALs (/.../{local_token_id}).
                    let local_token_ids = TokenIds::from_global_range(
                        record.kc_id as u128,
                        global_start,
                        global_end,
                        global_burned,
                    );
                    let start = local_token_ids.start_token_id();
                    let end = local_token_ids.end_token_id();
                    let burned = local_token_ids.burned();

                    let latest_merkle_root = merkle_result
                        .as_bytes32_hex()
                        .unwrap_or_else(|| format!("0x{}", to_hex_string(record.merkle_root)));
                    let burned_encoding = encode_burned_ids(start, end, burned);

                    record.kc_state_metadata = Some(KcHydratedStateMetadata {
                        range_start_token_id: start,
                        range_end_token_id: end,
                        burned_mode: burned_encoding.mode as u32,
                        burned_payload: burned_encoding.payload,
                        end_epoch,
                        latest_merkle_root,
                    });
                }
            }
            Err(error) => {
                tracing::warn!(
                    blockchain = %blockchain_id,
                    batch_size = chunk.len(),
                    error = %error,
                    "Failed to hydrate KC state metadata from chain"
                );
            }
        }
    }
}

pub(crate) async fn upsert_kc_chain_metadata_record(
    repository: &KcChainMetadataRepository,
    blockchain_id: &str,
    source: &str,
    record: &KcChainMetadataRecord,
) -> Result<(), RepositoryError> {
    let contract_address_str = format!("{:?}", record.contract_address);
    let transaction_hash_str = format!("{:#x}", record.transaction_hash);

    repository
        .upsert_core_metadata(
            blockchain_id,
            &contract_address_str,
            record.kc_id,
            record.publisher_address.as_deref(),
            record.block_number,
            &transaction_hash_str,
            record.block_timestamp,
            &record.publish_operation_id,
            Some(source),
        )
        .await?;

    if let Some(kc_state_metadata) = record.kc_state_metadata.as_ref() {
        repository
            .upsert_kc_state_metadata(
                blockchain_id,
                &contract_address_str,
                record.kc_id,
                kc_state_metadata.range_start_token_id,
                kc_state_metadata.range_end_token_id,
                kc_state_metadata.burned_mode,
                kc_state_metadata.burned_payload.as_slice(),
                kc_state_metadata.end_epoch,
                &kc_state_metadata.latest_merkle_root,
                record.block_number,
                Some(source),
            )
            .await?;
    }

    Ok(())
}
