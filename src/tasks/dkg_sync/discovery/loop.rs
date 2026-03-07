use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Utc;
use dkg_blockchain::{Address, BlockchainId};
use dkg_domain::canonical_evm_address;
use futures::StreamExt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::DiscoveryWorker;
use crate::tasks::dkg_sync::{DkgSyncConfig, DkgSyncDeps};

#[derive(Default)]
struct GapPassOutcome {
    any_chunk_processed: bool,
    had_errors: bool,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    discovery_worker: DiscoveryWorker,
    deps: DkgSyncDeps,
    config: DkgSyncConfig,
    reorg_buffer_blocks: u64,
    blockchain_id: BlockchainId,
    contract_addresses: Vec<Address>,
    notify: Arc<Notify>,
    shutdown: CancellationToken,
) {
    let contract_scan_concurrency = config.discovery.max_contract_concurrency.max(1);
    let high_watermark = config.discovery.queue_high_kc_watermark.max(1);
    let low_watermark = config.discovery.queue_low_kc_watermark.max(1);
    let error_retry_period =
        Duration::from_secs(config.discovery.metadata_error_retry_interval_secs.max(1));
    let live_poll_period = Duration::from_secs(config.discovery.live_poll_interval_secs.max(1));
    let mut gap_paused_by_backpressure = false;
    let mut historical_complete = false;

    let pinned_tip = loop {
        if shutdown.is_cancelled() {
            return;
        }

        if let Some(tip) = read_target_tip(&deps, &blockchain_id, reorg_buffer_blocks).await {
            break tip;
        }

        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(error_retry_period) => {}
        }
    };
    tracing::info!(
        blockchain_id = %blockchain_id,
        pinned_tip,
        contracts = contract_addresses.len(),
        "DKG sync discovery initialized with pinned historical tip"
    );
    let pinned_latest_kc_id_by_contract = loop {
        if shutdown.is_cancelled() {
            return;
        }

        if let Some(pinned_kc_ids) = read_pinned_latest_kc_ids(
            &deps,
            &blockchain_id,
            &contract_addresses,
            contract_scan_concurrency,
        )
        .await
        {
            break pinned_kc_ids;
        }

        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = tokio::time::sleep(error_retry_period) => {}
        }
    };
    tracing::info!(
        blockchain_id = %blockchain_id,
        contracts = contract_addresses.len(),
        resolved_contracts = pinned_latest_kc_id_by_contract.len(),
        "DKG sync discovery pinned latest KC ids for historical pass"
    );

    loop {
        if shutdown.is_cancelled() {
            break;
        }

        let Some(live_target_tip) =
            read_target_tip(&deps, &blockchain_id, reorg_buffer_blocks).await
        else {
            tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(error_retry_period) => {}
            }
            continue;
        };

        let mut any_chunk_processed = run_live_pass(
            &discovery_worker,
            &blockchain_id,
            &contract_addresses,
            contract_scan_concurrency,
            pinned_tip,
            live_target_tip,
        )
        .await;
        if any_chunk_processed {
            notify.notify_waiters();
        }

        let queue_total = active_queue_count(
            &deps,
            config.queue_processor.max_retry_attempts,
            &blockchain_id,
        )
        .await;
        let gap_allowed = should_run_gap_pass(
            &blockchain_id,
            queue_total,
            high_watermark,
            low_watermark,
            &mut gap_paused_by_backpressure,
        );

        if !historical_complete && gap_allowed {
            let gap_outcome = run_gap_pass(
                &discovery_worker,
                &blockchain_id,
                &contract_addresses,
                contract_scan_concurrency,
                pinned_tip,
                &pinned_latest_kc_id_by_contract,
            )
            .await;
            if gap_outcome.any_chunk_processed {
                any_chunk_processed = true;
                notify.notify_waiters();
            } else if !gap_outcome.had_errors {
                historical_complete = true;
                tracing::info!(
                    blockchain_id = %blockchain_id,
                    pinned_tip,
                    "DKG sync historical discovery reached pinned tip; stopping backfill lane"
                );
            }
        }

        if !any_chunk_processed {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(live_poll_period) => {}
            }
        }
    }
}

async fn read_target_tip(
    deps: &DkgSyncDeps,
    blockchain_id: &BlockchainId,
    reorg_buffer_blocks: u64,
) -> Option<u64> {
    match deps
        .blockchain_manager
        .get_block_number(blockchain_id)
        .await
    {
        Ok(block) => Some(block.saturating_sub(reorg_buffer_blocks)),
        Err(error) => {
            tracing::error!(
                blockchain_id = %blockchain_id,
                error = %error,
                "Failed to read chain tip for DKG sync discovery"
            );
            None
        }
    }
}

async fn read_pinned_latest_kc_ids(
    deps: &DkgSyncDeps,
    blockchain_id: &BlockchainId,
    contract_addresses: &[Address],
    contract_scan_concurrency: usize,
) -> Option<HashMap<String, u64>> {
    let results: Vec<(Address, Result<u64, dkg_blockchain::BlockchainError>)> =
        futures::stream::iter(contract_addresses.iter().copied().map(
            |contract_address| async move {
                (
                    contract_address,
                    deps.blockchain_manager
                        .get_latest_knowledge_collection_id(blockchain_id, contract_address)
                        .await,
                )
            },
        ))
        .buffer_unordered(contract_scan_concurrency)
        .collect()
        .await;

    let mut pinned_latest_kc_id_by_contract = HashMap::with_capacity(results.len());
    let mut had_errors = false;
    for (contract_address, result) in results {
        let contract_addr_str = canonical_evm_address(&contract_address);
        match result {
            Ok(latest_kc_id) => {
                pinned_latest_kc_id_by_contract.insert(contract_addr_str, latest_kc_id);
            }
            Err(error) => {
                had_errors = true;
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    error = %error,
                    "Failed to pin latest KC id for contract"
                );
            }
        }
    }

    if had_errors {
        tracing::warn!(
            blockchain_id = %blockchain_id,
            resolved_contracts = pinned_latest_kc_id_by_contract.len(),
            expected_contracts = contract_addresses.len(),
            "Failed to pin latest KC ids for all contracts; retrying startup pinning"
        );
        None
    } else {
        Some(pinned_latest_kc_id_by_contract)
    }
}

async fn run_live_pass(
    discovery_worker: &DiscoveryWorker,
    blockchain_id: &BlockchainId,
    contract_addresses: &[Address],
    contract_scan_concurrency: usize,
    pinned_tip: u64,
    target_tip: u64,
) -> bool {
    let mut any_chunk_processed = false;
    let live_results: Vec<(Address, Result<_, _>)> =
        futures::stream::iter(contract_addresses.iter().copied().map(
            |contract_address| async move {
                (
                    contract_address,
                    discovery_worker
                        .discover_live_once(blockchain_id, contract_address, pinned_tip, target_tip)
                        .await,
                )
            },
        ))
        .buffer_unordered(contract_scan_concurrency)
        .collect()
        .await;

    for (contract_address, result) in live_results {
        let contract_addr_str = canonical_evm_address(&contract_address);
        match result {
            Ok(discovery_result) => {
                if discovery_result.chunk_processed {
                    any_chunk_processed = true;
                }
            }
            Err(error) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    error = %error,
                    "DKG sync discovery failed for contract live pass"
                );
            }
        }
    }

    any_chunk_processed
}

async fn run_gap_pass(
    discovery_worker: &DiscoveryWorker,
    blockchain_id: &BlockchainId,
    contract_addresses: &[Address],
    contract_scan_concurrency: usize,
    target_tip: u64,
    pinned_latest_kc_id_by_contract: &HashMap<String, u64>,
) -> GapPassOutcome {
    let mut outcome = GapPassOutcome::default();
    let gap_results: Vec<(Address, Result<_, _>)> =
        futures::stream::iter(contract_addresses.iter().copied().map(|contract_address| {
            let contract_addr_str = canonical_evm_address(&contract_address);
            let pinned_latest_kc_id = pinned_latest_kc_id_by_contract
                .get(&contract_addr_str)
                .copied()
                .expect("all contracts must have pinned latest KC id before discovery loop starts");
            async move {
                (
                    contract_address,
                    discovery_worker
                        .discover_contract_once(
                            blockchain_id,
                            contract_address,
                            target_tip,
                            pinned_latest_kc_id,
                        )
                        .await,
                )
            }
        }))
        .buffer_unordered(contract_scan_concurrency)
        .collect()
        .await;

    for (contract_address, result) in gap_results {
        let contract_addr_str = canonical_evm_address(&contract_address);
        match result {
            Ok(discovery_result) => {
                if discovery_result.chunk_processed {
                    outcome.any_chunk_processed = true;
                }
            }
            Err(error) => {
                outcome.had_errors = true;
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    contract = %contract_addr_str,
                    error = %error,
                    "DKG sync discovery failed for contract gap pass"
                );
            }
        }
    }

    outcome
}

fn should_run_gap_pass(
    blockchain_id: &BlockchainId,
    queue_total: u64,
    high_watermark: u64,
    low_watermark: u64,
    gap_paused_by_backpressure: &mut bool,
) -> bool {
    if *gap_paused_by_backpressure {
        if queue_total > low_watermark {
            return false;
        }

        *gap_paused_by_backpressure = false;
        tracing::info!(
            blockchain_id = %blockchain_id,
            queue_total,
            low_watermark,
            "DKG sync discovery resuming gap backfill after queue pressure dropped"
        );
    }

    if queue_total >= high_watermark {
        *gap_paused_by_backpressure = true;
        tracing::info!(
            blockchain_id = %blockchain_id,
            queue_total,
            high_watermark,
            "DKG sync discovery pausing gap backfill due to queue pressure"
        );
        return false;
    }

    true
}

async fn active_queue_count(
    deps: &DkgSyncDeps,
    max_retry_attempts: u32,
    blockchain_id: &BlockchainId,
) -> u64 {
    let max_retry_attempts = max_retry_attempts.max(1);
    let now_ts = Utc::now().timestamp();
    let due = deps
        .kc_sync_repository
        .count_queue_due_for_blockchain(blockchain_id.as_str(), now_ts, max_retry_attempts)
        .await;
    let retrying = deps
        .kc_sync_repository
        .count_queue_retrying_for_blockchain(blockchain_id.as_str(), now_ts, max_retry_attempts)
        .await;

    match (due, retrying) {
        (Ok(due), Ok(retrying)) => due.saturating_add(retrying),
        (due_result, retry_result) => {
            if let Err(error) = due_result {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to read due queue count in DKG sync task"
                );
            }
            if let Err(error) = retry_result {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to read retry queue count in DKG sync task"
                );
            }
            0
        }
    }
}
