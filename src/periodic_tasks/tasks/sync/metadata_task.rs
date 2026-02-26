use std::{
    collections::HashSet,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use dkg_blockchain::{
    Address, BlockchainId, ContractEvent, ContractName, decode_contract_event,
    monitored_contract_events,
};
use dkg_observability as observability;
use dkg_repository::GapBoundaries;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::SyncConfig;
use crate::{periodic_tasks::SyncDeps, periodic_tasks::runner::run_with_shutdown};

pub(crate) struct MetadataSyncTask {
    config: SyncConfig,
    deps: SyncDeps,
    last_non_tail_scan_at_unix_secs: AtomicU64,
}

#[derive(Debug, Error)]
enum MetadataSyncError {
    #[error("Failed to load metadata sync progress")]
    LoadMetadataProgress(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to update metadata sync progress")]
    UpdateMetadataProgress(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to fetch metadata events chunk")]
    FetchMetadataChunk(#[source] dkg_blockchain::BlockchainError),
    #[error("Failed to fetch deployment block")]
    FindDeploymentBlock(#[source] dkg_blockchain::BlockchainError),
    #[error("Failed to upsert core metadata")]
    UpsertCoreMetadata(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to enqueue KC IDs")]
    EnqueueKcs(#[source] dkg_repository::error::RepositoryError),
    #[error("Failed to query KC ID gap boundaries")]
    FindGapBoundaries(#[source] dkg_repository::error::RepositoryError),
}

#[derive(Default)]
struct ContractMetadataSyncResult {
    metadata_events_found: u64,
    cursor_advanced: bool,
    gap_ranges_detected: u64,
}

/// An inclusive block range [start, end] to scan for KC events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeType {
    Leading,
    Internal,
    Tail,
}

impl RangeType {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Leading => "leading",
            Self::Internal => "internal",
            Self::Tail => "tail",
        }
    }
}

/// An inclusive block range [start, end] to scan for KC events.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockRange {
    start: u64,
    end: u64,
    range_type: RangeType,
}

/// Output of `compute_scan_ranges`.
struct ScanPlan {
    ranges: Vec<BlockRange>,
}

/// Compute the set of block ranges to scan given the gap boundaries from the DB,
/// the deployment block, the current chain tip, and the last scanned block cursor.
///
/// Rules:
/// - Leading and internal gaps are NOT clamped by cursor (always fully re-scanned).
/// - Only the tail gap is clamped: `start = max(tail_start_block, cursor + 1)`.
/// - Ranges are inclusive; a single-block range (start == end) is valid.
/// - Ranges where start > end are skipped.
/// - `deployment_block = None` means the deployment block could not be resolved;
///   empty-DB and leading-gap ranges are skipped in that case.
fn compute_scan_ranges(
    boundaries: &GapBoundaries,
    deployment_block: Option<u64>,
    target_tip: u64,
    cursor: u64,
) -> ScanPlan {
    let starts = &boundaries.starts_of_runs;
    let ends = &boundaries.ends_of_runs;
    let mut ranges = Vec::new();

    if starts.is_empty() {
        // No KCs in DB — scan from deployment to tip (this range is the tail).
        if let Some(dep) = deployment_block {
            let start = dep.max(cursor.saturating_add(1));
            if start <= target_tip {
                ranges.push(BlockRange {
                    start,
                    end: target_tip,
                    range_type: RangeType::Tail,
                });
            }
        }
        return ScanPlan { ranges };
    }

    // Leading gap: first known KC doesn't have ID 1.
    let (first_kc_id, first_kc_block) = starts[0];
    if first_kc_id > 1 {
        if let Some(dep) = deployment_block {
            if dep <= first_kc_block {
                ranges.push(BlockRange {
                    start: dep,
                    end: first_kc_block,
                    range_type: RangeType::Leading,
                });
            }
        }
        // deployment_block == None: leading-gap skipped (no known lower bound).
    }

    // Internal gaps: pair ends[i] with starts[i+1].
    let n = ends.len();
    for i in 0..n.saturating_sub(1) {
        let (_, lower_block) = ends[i];
        let (_, upper_block) = starts[i + 1];
        if lower_block <= upper_block {
            ranges.push(BlockRange {
                start: lower_block,
                end: upper_block,
                range_type: RangeType::Internal,
            });
        }
    }

    // Tail gap: from last known KC block to the current tip, clamped by cursor.
    let (_, tail_start_block): (u64, u64) = ends[n - 1];
    let tail_start = tail_start_block.max(cursor.saturating_add(1));
    if tail_start <= target_tip {
        ranges.push(BlockRange {
            start: tail_start,
            end: target_tip,
            range_type: RangeType::Tail,
        });
    }

    ScanPlan { ranges }
}

impl MetadataSyncTask {
    pub(crate) fn new(deps: SyncDeps, config: SyncConfig) -> Self {
        Self {
            config,
            deps,
            last_non_tail_scan_at_unix_secs: AtomicU64::new(0),
        }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("sync_metadata", shutdown, || self.execute(blockchain_id)).await;
    }

    #[tracing::instrument(name = "periodic_tasks.sync_metadata", skip(self), fields(blockchain_id = %blockchain_id))]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let idle_period = Duration::from_secs(self.config.sync_idle_sleep_secs.max(1));
        let recheck_secs = self.config.metadata_gap_recheck_interval_secs.max(1);
        let recheck_period = Duration::from_secs(recheck_secs);
        let now_unix_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last_non_tail_scan = self.last_non_tail_scan_at_unix_secs.load(Ordering::Relaxed);
        let scan_non_tail_gaps = now_unix_secs.saturating_sub(last_non_tail_scan) >= recheck_secs;

        let current_block = match self
            .deps
            .blockchain_manager
            .get_block_number(blockchain_id)
            .await
        {
            Ok(block) => block,
            Err(error) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to resolve current block for metadata sync cycle"
                );
                return idle_period;
            }
        };
        let target_tip = current_block.saturating_sub(self.config.head_safety_blocks);

        let contract_addresses = match self
            .deps
            .blockchain_manager
            .get_all_contract_addresses(blockchain_id, &ContractName::KnowledgeCollectionStorage)
            .await
        {
            Ok(addresses) => addresses,
            Err(error) => {
                tracing::error!(
                    blockchain_id = %blockchain_id,
                    error = %error,
                    "Failed to get KC storage contract addresses for metadata sync"
                );
                return idle_period;
            }
        };

        let mut any_cursor_advanced = false;

        for contract_address in contract_addresses {
            match self
                .sync_contract(
                    blockchain_id,
                    contract_address,
                    target_tip,
                    scan_non_tail_gaps,
                )
                .await
            {
                Ok(result) => {
                    any_cursor_advanced |= result.cursor_advanced;
                }
                Err(error) => {
                    tracing::error!(
                        blockchain_id = %blockchain_id,
                        contract = ?contract_address,
                        error = %error,
                        "Failed metadata sync for contract"
                    );
                }
            }
        }

        if scan_non_tail_gaps {
            self.last_non_tail_scan_at_unix_secs
                .store(now_unix_secs, Ordering::Relaxed);
        }

        // Keep hot loop only for tail cursor progress. Gap rescans run on recheck cadence.
        if any_cursor_advanced {
            return Duration::ZERO;
        }

        recheck_period
    }

    async fn sync_contract(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        target_tip: u64,
        scan_non_tail_gaps: bool,
    ) -> Result<ContractMetadataSyncResult, MetadataSyncError> {
        let contract_addr_str = format!("{:?}", contract_address);
        let mut result = ContractMetadataSyncResult::default();

        let cursor = self
            .deps
            .kc_sync_repository
            .get_metadata_progress(blockchain_id.as_str(), &contract_addr_str)
            .await
            .map_err(MetadataSyncError::LoadMetadataProgress)?;

        // Query gap boundaries first so we know whether deployment block is needed.
        let boundaries = self
            .deps
            .kc_chain_metadata_repository
            .find_gap_boundaries(blockchain_id.as_str(), &contract_addr_str)
            .await
            .map_err(MetadataSyncError::FindGapBoundaries)?;

        // Resolve the deployment block only when required:
        // - DB is empty (we need a start anchor for the tail scan), or
        // - A leading gap exists (first KC ID > 1, so we need the lower bound).
        // For internal and tail gaps the deployment block is irrelevant.
        let needs_deployment_block = boundaries.starts_of_runs.is_empty()
            || boundaries.starts_of_runs.first().is_some_and(|pair| pair.0 > 1);

        let deployment_block: Option<u64> = if needs_deployment_block {
            match self
                .deps
                .blockchain_manager
                .find_contract_deployment_block(blockchain_id, contract_address, target_tip)
                .await
                .map_err(MetadataSyncError::FindDeploymentBlock)?
            {
                Some(block) => Some(block),
                None if boundaries.starts_of_runs.is_empty() => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        "No deployment block found and DB is empty; skipping metadata sync"
                    );
                    return Ok(result);
                }
                None => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        contract = %contract_addr_str,
                        "Deployment block unavailable; leading-gap scan will be skipped"
                    );
                    None
                }
            }
        } else {
            None
        };

        let mut plan = compute_scan_ranges(&boundaries, deployment_block, target_tip, cursor);

        result.gap_ranges_detected = plan.ranges.len() as u64;

        observability::record_sync_metadata_gaps_detected(
            blockchain_id.as_str(),
            &contract_addr_str,
            result.gap_ranges_detected,
        );

        if !scan_non_tail_gaps {
            if let Some(tail_range) = plan
                .ranges
                .iter()
                .find(|range| range.range_type == RangeType::Tail)
                .cloned()
            {
                plan.ranges = vec![tail_range];
            } else {
                plan.ranges.clear();
            }
        }

        if plan.ranges.is_empty() {
            observability::record_sync_metadata_cursor(
                blockchain_id.as_str(),
                &contract_addr_str,
                cursor,
            );
            observability::record_sync_metadata_cursor_lag(
                blockchain_id.as_str(),
                &contract_addr_str,
                target_tip.saturating_sub(cursor),
            );
            return Ok(result);
        }

        let event_signatures = monitored_contract_events()
            .get(&ContractName::KnowledgeCollectionStorage)
            .cloned()
            .unwrap_or_default();
        let chunk_size = self.config.metadata_backfill_block_batch_size.max(1);

        let mut current_cursor = cursor;

        for scan_range in plan.ranges.iter() {
            // Cursor advances only for tail scans.
            let is_tail = scan_range.range_type == RangeType::Tail;
            let mut chunk_from = scan_range.start;

            while chunk_from <= scan_range.end {
                let chunk_to =
                    scan_range.end.min(chunk_from.saturating_add(chunk_size.saturating_sub(1)));
                let chunk_started = std::time::Instant::now();

                let logs = self
                    .deps
                    .blockchain_manager
                    .get_event_logs_for_address(
                        blockchain_id,
                        ContractName::KnowledgeCollectionStorage,
                        contract_address,
                        &event_signatures,
                        chunk_from,
                        chunk_to,
                    )
                    .await
                    .map_err(MetadataSyncError::FetchMetadataChunk)?;

                let mut discovered_ids = HashSet::new();
                let mut chunk_events_found = 0_usize;

                for log in logs {
                    let Some(ContractEvent::KnowledgeCollectionCreated {
                        event,
                        contract_address: event_contract_address,
                        transaction_hash,
                        block_number,
                        block_timestamp,
                    }) = decode_contract_event(log.contract_name(), log.log())
                    else {
                        continue;
                    };

                    let kc_id_u128: u128 = event.id.to();
                    let Ok(kc_id) = u64::try_from(kc_id_u128) else {
                        continue;
                    };
                    let Some(tx_hash) = transaction_hash else {
                        continue;
                    };

                    let publisher_address = self
                        .resolve_kc_publisher(
                            blockchain_id,
                            event_contract_address,
                            kc_id_u128,
                            tx_hash,
                        )
                        .await;

                    let tx_hash_str = format!("{:#x}", tx_hash);
                    self.deps
                        .kc_chain_metadata_repository
                        .upsert_core_metadata(
                            blockchain_id.as_str(),
                            &contract_addr_str,
                            kc_id,
                            publisher_address.as_deref(),
                            block_number,
                            &tx_hash_str,
                            block_timestamp,
                            &event.publishOperationId,
                            Some("sync_metadata_backfill"),
                        )
                        .await
                        .map_err(MetadataSyncError::UpsertCoreMetadata)?;

                    discovered_ids.insert(kc_id);
                    result.metadata_events_found =
                        result.metadata_events_found.saturating_add(1);
                    chunk_events_found = chunk_events_found.saturating_add(1);
                }

                if !discovered_ids.is_empty() {
                    let mut ids: Vec<u64> = discovered_ids.into_iter().collect();
                    ids.sort_unstable();
                    self.deps
                        .kc_sync_repository
                        .enqueue_kcs(blockchain_id.as_str(), &contract_addr_str, &ids)
                        .await
                        .map_err(MetadataSyncError::EnqueueKcs)?;
                }

                // Only advance the persistent cursor during tail scans.
                if is_tail {
                    self.deps
                        .kc_sync_repository
                        .upsert_metadata_progress(
                            blockchain_id.as_str(),
                            &contract_addr_str,
                            chunk_to,
                        )
                        .await
                        .map_err(MetadataSyncError::UpdateMetadataProgress)?;
                    current_cursor = chunk_to;
                    result.cursor_advanced = true;
                }

                observability::record_sync_metadata_backfill_chunk(
                    blockchain_id.as_str(),
                    "ok",
                    scan_range.range_type.as_str(),
                    chunk_started.elapsed(),
                    chunk_to.saturating_sub(chunk_from).saturating_add(1),
                    chunk_events_found,
                );

                chunk_from = chunk_to.saturating_add(1);
            }
        }

        observability::record_sync_metadata_events_found_total(
            blockchain_id.as_str(),
            result.metadata_events_found,
        );
        observability::record_sync_metadata_cursor(
            blockchain_id.as_str(),
            &contract_addr_str,
            current_cursor,
        );
        observability::record_sync_metadata_cursor_lag(
            blockchain_id.as_str(),
            &contract_addr_str,
            target_tip.saturating_sub(current_cursor),
        );

        Ok(result)
    }

    async fn resolve_kc_publisher(
        &self,
        blockchain_id: &BlockchainId,
        contract_address: Address,
        kc_id: u128,
        tx_hash: dkg_blockchain::B256,
    ) -> Option<String> {
        match self
            .deps
            .blockchain_manager
            .get_knowledge_collection_publisher(blockchain_id, contract_address, kc_id)
            .await
        {
            Ok(Some(address)) => Some(format!("{:?}", address)),
            _ => match self
                .deps
                .blockchain_manager
                .get_transaction_sender(blockchain_id, tx_hash)
                .await
            {
                Ok(Some(address)) => Some(format!("{:?}", address)),
                _ => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dkg_repository::GapBoundaries;

    fn boundaries(ends: Vec<(u64, u64)>, starts: Vec<(u64, u64)>) -> GapBoundaries {
        GapBoundaries { ends_of_runs: ends, starts_of_runs: starts }
    }

    fn br(start: u64, end: u64, range_type: RangeType) -> BlockRange {
        BlockRange {
            start,
            end,
            range_type,
        }
    }

    #[test]
    fn empty_db_scans_deployment_to_tip() {
        let b = boundaries(vec![], vec![]);
        let plan = compute_scan_ranges(&b, Some(100), 500, 0);
        assert_eq!(plan.ranges, vec![br(100, 500, RangeType::Tail)]);
    }

    #[test]
    fn empty_db_with_cursor_clamps_start() {
        let b = boundaries(vec![], vec![]);
        let plan = compute_scan_ranges(&b, Some(100), 500, 300);
        assert_eq!(plan.ranges, vec![br(301, 500, RangeType::Tail)]);
    }

    #[test]
    fn empty_db_no_deployment_block_yields_empty() {
        let b = boundaries(vec![], vec![]);
        let plan = compute_scan_ranges(&b, None, 500, 0);
        assert!(plan.ranges.is_empty());
    }

    #[test]
    fn contiguous_ids_only_tail() {
        // KCs [1-3], no gaps, last block at 200, cursor at 150.
        let b = boundaries(vec![(3, 200)], vec![(1, 50)]);
        let plan = compute_scan_ranges(&b, None, 500, 150);
        // No leading gap (starts[0].kc_id == 1). No internal gaps. Tail: max(200, 151) = 200.
        assert_eq!(plan.ranges, vec![br(200, 500, RangeType::Tail)]);
    }

    #[test]
    fn contiguous_ids_cursor_past_tail_start() {
        let b = boundaries(vec![(3, 200)], vec![(1, 50)]);
        let plan = compute_scan_ranges(&b, None, 500, 350);
        assert_eq!(plan.ranges, vec![br(351, 500, RangeType::Tail)]);
    }

    #[test]
    fn cursor_at_tip_yields_empty_tail() {
        let b = boundaries(vec![(3, 200)], vec![(1, 50)]);
        let plan = compute_scan_ranges(&b, None, 500, 500);
        // Tail skipped; no ranges at all.
        assert_eq!(plan.ranges, vec![]);
    }

    #[test]
    fn internal_gaps_not_clamped_by_cursor() {
        // KCs [1-3], [7-9], [12-13]. Cursor = 600 (past all gap blocks).
        // ends: [(3,100),(9,300),(13,450)]
        // starts: [(1,50),(7,200),(12,400)]
        let b = boundaries(
            vec![(3, 100), (9, 300), (13, 450)],
            vec![(1, 50), (7, 200), (12, 400)],
        );
        let plan = compute_scan_ranges(&b, None, 700, 600);
        // No leading gap (starts[0].kc_id == 1).
        // Internal gap 1: ends[0]=(3,100) ↔ starts[1]=(7,200) → [100,200]
        // Internal gap 2: ends[1]=(9,300) ↔ starts[2]=(12,400) → [300,400]
        // Tail: max(450, 601) = 601 → [601, 700]
        assert_eq!(
            plan.ranges,
            vec![
                br(100, 200, RangeType::Internal),
                br(300, 400, RangeType::Internal),
                br(601, 700, RangeType::Tail),
            ]
        );
    }

    #[test]
    fn internal_gaps_with_cursor_at_tip_no_tail() {
        // Same gaps, cursor already at tip — tail skipped, internal gaps still scanned.
        let b = boundaries(
            vec![(3, 100), (9, 300), (13, 450)],
            vec![(1, 50), (7, 200), (12, 400)],
        );
        let plan = compute_scan_ranges(&b, None, 700, 700);
        assert_eq!(
            plan.ranges,
            vec![
                br(100, 200, RangeType::Internal),
                br(300, 400, RangeType::Internal),
                // No tail — cursor is at tip.
            ]
        );
    }

    #[test]
    fn leading_gap_uses_deployment_block() {
        // First KC in DB is ID 5 at block 300. Deployment at 100.
        let b = boundaries(vec![(5, 300)], vec![(5, 300)]);
        let plan = compute_scan_ranges(&b, Some(100), 600, 0);
        // Leading gap: [100, 300]
        // Tail: max(300, 1) = 300 → [300, 600]
        assert_eq!(
            plan.ranges,
            vec![
                br(100, 300, RangeType::Leading),
                br(300, 600, RangeType::Tail),
            ]
        );
    }

    #[test]
    fn leading_gap_skipped_when_no_deployment_block() {
        // Leading gap exists but deployment block unavailable.
        let b = boundaries(vec![(5, 300)], vec![(5, 300)]);
        let plan = compute_scan_ranges(&b, None, 600, 0);
        // Leading gap skipped; only tail remains.
        assert_eq!(plan.ranges, vec![br(300, 600, RangeType::Tail)]);
    }

    #[test]
    fn single_block_range_is_valid() {
        // Three runs [1-3],[7-9],[12-13] where gap boundaries land on the same block.
        // ends: [(3,150),(9,300),(13,450)], starts: [(1,50),(7,150),(12,300)]
        let b = boundaries(
            vec![(3, 150), (9, 300), (13, 450)],
            vec![(1, 50), (7, 150), (12, 300)],
        );
        let plan = compute_scan_ranges(&b, None, 500, 0);
        // Internal gap 1: ends[0]=(3,150) ↔ starts[1]=(7,150) → [150, 150] single-block
        // Internal gap 2: ends[1]=(9,300) ↔ starts[2]=(12,300) → [300, 300] single-block
        // Tail: [450, 500]
        assert!(plan
            .ranges
            .iter()
            .any(|r| r.start == 150 && r.end == 150 && r.range_type == RangeType::Internal));
        assert!(plan
            .ranges
            .iter()
            .any(|r| r.start == 300 && r.end == 300 && r.range_type == RangeType::Internal));
        assert!(plan
            .ranges
            .iter()
            .any(|r| r.start == 450 && r.end == 500 && r.range_type == RangeType::Tail));
    }
}
