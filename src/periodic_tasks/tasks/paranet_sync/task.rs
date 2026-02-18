use std::{collections::HashMap, sync::Arc, time::Duration};

use dkg_blockchain::BlockchainId;
use dkg_domain::{AccessPolicy, construct_paranet_id, derive_ual, parse_ual};
use dkg_repository::ParanetKcSyncEntry;
use dkg_triple_store::parse_metadata_from_triples;
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::ParanetSyncConfig;
use crate::{
    periodic_tasks::ParanetSyncDeps,
    periodic_tasks::runner::run_with_shutdown,
    services::{GetFetchRequest, GetFetchSource},
};

pub(crate) struct ParanetSyncTask {
    config: ParanetSyncConfig,
    deps: ParanetSyncDeps,
}

impl ParanetSyncTask {
    pub(crate) fn new(deps: ParanetSyncDeps, config: ParanetSyncConfig) -> Self {
        Self { config, deps }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        run_with_shutdown("paranet_sync", shutdown, || self.execute(blockchain_id)).await;
    }

    #[tracing::instrument(name = "periodic_tasks.paranet_sync", skip(self), fields(blockchain_id = %blockchain_id))]
    async fn execute(&self, blockchain_id: &BlockchainId) -> Duration {
        let interval = Duration::from_secs(self.config.interval_secs);

        if !self.config.enabled || self.config.sync_paranets.is_empty() {
            tracing::trace!(
                blockchain_id = %blockchain_id,
                enabled = self.config.enabled,
                configured_paranets = self.config.sync_paranets.len(),
                "Paranet sync disabled or no paranets configured"
            );
            return interval;
        }

        let cycle_start = std::time::Instant::now();
        let targets = self.resolve_targets(blockchain_id).await;
        if targets.is_empty() {
            tracing::trace!(
                blockchain_id = %blockchain_id,
                "No valid paranet sync targets for this blockchain"
            );
            return interval;
        }

        let mut discovered_total = 0u64;
        let mut enqueued_total = 0u64;
        for target in &targets {
            let (discovered, enqueued) = self.discover_paranet_kcs(blockchain_id, target).await;
            discovered_total = discovered_total.saturating_add(discovered);
            enqueued_total = enqueued_total.saturating_add(enqueued);
        }

        let due_batch = self.build_due_batch(blockchain_id, &targets).await;
        let batch_due = due_batch.len();

        let (synced, failed, retry_exhausted, local_hits, network_hits) = self
            .process_due_batch(blockchain_id, &targets, due_batch)
            .await;

        tracing::info!(
            blockchain_id = %blockchain_id,
            discovered = discovered_total,
            enqueued = enqueued_total,
            batch_due = batch_due,
            synced,
            failed,
            retry_exhausted,
            local_hits,
            network_hits,
            cycle_ms = cycle_start.elapsed().as_millis() as u64,
            "Paranet sync cycle completed"
        );

        interval
    }

    async fn resolve_targets(&self, blockchain_id: &BlockchainId) -> Vec<ParanetSyncTarget> {
        let mut targets = Vec::new();

        for paranet_ual in &self.config.sync_paranets {
            let parsed = match parse_ual(paranet_ual) {
                Ok(parsed) => parsed,
                Err(e) => {
                    tracing::warn!(
                        paranet_ual = %paranet_ual,
                        error = %e,
                        "Invalid paranet UAL in config"
                    );
                    continue;
                }
            };

            if &parsed.blockchain != blockchain_id {
                continue;
            }

            let Some(ka_id) = parsed.knowledge_asset_id else {
                tracing::warn!(
                    paranet_ual = %paranet_ual,
                    "Paranet UAL missing knowledge asset ID in config"
                );
                continue;
            };

            let paranet_id =
                construct_paranet_id(parsed.contract, parsed.knowledge_collection_id, ka_id);
            let access_policy = match self
                .deps
                .blockchain_manager
                .get_nodes_access_policy(blockchain_id, paranet_id)
                .await
            {
                Ok(policy) => policy,
                Err(e) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        paranet_ual = %paranet_ual,
                        error = %e,
                        "Failed to resolve paranet access policy"
                    );
                    continue;
                }
            };

            targets.push(ParanetSyncTarget {
                paranet_ual: paranet_ual.clone(),
                paranet_id: format!("{paranet_id:?}"),
                paranet_id_b256: paranet_id,
                access_policy,
            });
        }

        targets
    }

    async fn discover_paranet_kcs(
        &self,
        blockchain_id: &BlockchainId,
        target: &ParanetSyncTarget,
    ) -> (u64, u64) {
        let repo = &self.deps.paranet_kc_sync_repository;
        let discovered = match repo.count_discovered(&target.paranet_ual).await {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    paranet = %target.paranet_ual,
                    error = %e,
                    "Failed to count discovered paranet KCs"
                );
                return (0, 0);
            }
        };

        let on_chain_count = match self
            .deps
            .blockchain_manager
            .get_paranet_knowledge_collection_count(blockchain_id, target.paranet_id_b256)
            .await
        {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    paranet = %target.paranet_ual,
                    error = %e,
                    "Failed to fetch paranet KC count"
                );
                return (0, 0);
            }
        };

        if on_chain_count <= discovered {
            return (0, 0);
        }

        let mut offset = discovered;
        let mut discovered_now = 0u64;
        let mut enqueued_now = 0u64;
        let page_limit = self.config.batch_size.max(1) as u64;

        while offset < on_chain_count {
            let limit = page_limit.min(on_chain_count.saturating_sub(offset));
            let locators = match self
                .deps
                .blockchain_manager
                .get_paranet_knowledge_collection_locators_with_pagination(
                    blockchain_id,
                    target.paranet_id_b256,
                    offset,
                    limit,
                )
                .await
            {
                Ok(locators) => locators,
                Err(e) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        paranet = %target.paranet_ual,
                        offset,
                        limit,
                        error = %e,
                        "Failed to fetch paranet KC locators page"
                    );
                    break;
                }
            };

            if locators.is_empty() {
                break;
            }

            let kc_uals: Vec<String> = locators
                .iter()
                .map(|locator| {
                    derive_ual(
                        blockchain_id,
                        &locator.knowledge_collection_storage_contract,
                        locator.knowledge_collection_token_id,
                        None,
                    )
                })
                .collect();

            if let Err(e) = repo
                .enqueue_locators(
                    &target.paranet_ual,
                    blockchain_id.as_str(),
                    &target.paranet_id,
                    &kc_uals,
                )
                .await
            {
                tracing::warn!(
                    blockchain_id = %blockchain_id,
                    paranet = %target.paranet_ual,
                    error = %e,
                    "Failed to enqueue paranet KC locators"
                );
                break;
            }

            let step = kc_uals.len() as u64;
            discovered_now = discovered_now.saturating_add(step);
            enqueued_now = enqueued_now.saturating_add(step);
            offset = offset.saturating_add(step);
        }

        (discovered_now, enqueued_now)
    }

    async fn build_due_batch(
        &self,
        blockchain_id: &BlockchainId,
        targets: &[ParanetSyncTarget],
    ) -> Vec<ParanetKcSyncEntry> {
        let repo = &self.deps.paranet_kc_sync_repository;
        let now_ts = chrono::Utc::now().timestamp();
        let total_limit = self.config.batch_size.max(1);
        let per_paranet_limit = (total_limit + targets.len().saturating_sub(1)) / targets.len();

        let mut per_paranet: Vec<Vec<ParanetKcSyncEntry>> = Vec::with_capacity(targets.len());
        for target in targets {
            match repo
                .get_due_pending_batch(
                    blockchain_id.as_str(),
                    &target.paranet_ual,
                    now_ts,
                    self.config.retries_limit,
                    per_paranet_limit as u64,
                )
                .await
            {
                Ok(rows) => per_paranet.push(rows),
                Err(e) => {
                    tracing::warn!(
                        blockchain_id = %blockchain_id,
                        paranet = %target.paranet_ual,
                        error = %e,
                        "Failed to load due paranet sync batch"
                    );
                    per_paranet.push(Vec::new());
                }
            }
        }

        // Round-robin merge to preserve fairness across paranets.
        let mut merged = Vec::new();
        let mut idx = 0usize;
        while merged.len() < total_limit {
            let mut added_any = false;
            for rows in &mut per_paranet {
                if idx < rows.len() {
                    merged.push(rows[idx].clone());
                    added_any = true;
                    if merged.len() >= total_limit {
                        break;
                    }
                }
            }

            if !added_any {
                break;
            }
            idx = idx.saturating_add(1);
        }

        merged
    }

    async fn process_due_batch(
        &self,
        blockchain_id: &BlockchainId,
        targets: &[ParanetSyncTarget],
        due_batch: Vec<ParanetKcSyncEntry>,
    ) -> (usize, usize, usize, usize, usize) {
        let target_map: HashMap<String, ParanetSyncTarget> = targets
            .iter()
            .map(|target| (target.paranet_ual.clone(), target.clone()))
            .collect();

        let paranet_kc_sync_repository = self.deps.paranet_kc_sync_repository.clone();
        let triple_store_service = Arc::clone(&self.deps.triple_store_service);
        let get_fetch_service = Arc::clone(&self.deps.get_fetch_service);
        let retries_limit = self.config.retries_limit;
        let retry_delay_secs = self.config.retry_delay_secs;
        let max_in_flight = self.config.max_in_flight.max(1);
        let blockchain_id = blockchain_id.clone();

        let stream = futures::stream::iter(due_batch.into_iter())
            .map(|row| {
                let target = target_map.get(&row.paranet_ual).cloned();
                let paranet_kc_sync_repository = paranet_kc_sync_repository.clone();
                let triple_store_service = Arc::clone(&triple_store_service);
                let get_fetch_service = Arc::clone(&get_fetch_service);
                let blockchain_id = blockchain_id.clone();

                async move {
                    let Some(target) = target else {
                        return RowOutcome::default();
                    };

                    let visibility = match target.access_policy {
                        AccessPolicy::Open => dkg_domain::Visibility::Public,
                        AccessPolicy::Permissioned => dkg_domain::Visibility::All,
                    };

                    let request = GetFetchRequest {
                        operation_id: Uuid::new_v4(),
                        ual: row.kc_ual.clone(),
                        include_metadata: true,
                        paranet_ual: Some(row.paranet_ual.clone()),
                        visibility,
                    };

                    let repo = paranet_kc_sync_repository;

                    let fetched = get_fetch_service.fetch(&request).await;
                    match fetched {
                        Ok(fetch_result) => {
                            let mut outcome = RowOutcome::default();
                            match fetch_result.source {
                                GetFetchSource::Local => outcome.local_hits = 1,
                                GetFetchSource::Network => outcome.network_hits = 1,
                            }

                            let metadata = fetch_result
                                .metadata
                                .as_ref()
                                .and_then(|triples| parse_metadata_from_triples(triples));

                            match triple_store_service
                                .insert_knowledge_collection(
                                    &row.kc_ual,
                                    &fetch_result.assertion,
                                    &metadata,
                                    Some(&row.paranet_ual),
                                )
                                .await
                            {
                                Ok(_) => {
                                    if let Err(e) =
                                        repo.mark_synced(&row.paranet_ual, &row.kc_ual).await
                                    {
                                        tracing::warn!(
                                            blockchain_id = %blockchain_id,
                                            paranet = %row.paranet_ual,
                                            kc_ual = %row.kc_ual,
                                            error = %e,
                                            "Failed to mark paranet KC as synced"
                                        );
                                    } else {
                                        outcome.synced = 1;
                                    }
                                }
                                Err(e) => {
                                    outcome.failed = 1;
                                    let now_ts = chrono::Utc::now().timestamp();
                                    if let Err(update_error) = repo
                                        .mark_failed_attempt(
                                            &row.paranet_ual,
                                            &row.kc_ual,
                                            now_ts,
                                            retry_delay_secs,
                                            &format!("Triple store insert failed: {}", e),
                                        )
                                        .await
                                    {
                                        tracing::warn!(
                                            blockchain_id = %blockchain_id,
                                            paranet = %row.paranet_ual,
                                            kc_ual = %row.kc_ual,
                                            error = %update_error,
                                            "Failed to update retry state after insert failure"
                                        );
                                    }

                                    if row.retry_count.saturating_add(1) >= retries_limit {
                                        outcome.retry_exhausted = 1;
                                    }
                                }
                            }
                            outcome
                        }
                        Err(error_message) => {
                            let mut outcome = RowOutcome {
                                failed: 1,
                                ..Default::default()
                            };
                            let now_ts = chrono::Utc::now().timestamp();
                            if let Err(e) = repo
                                .mark_failed_attempt(
                                    &row.paranet_ual,
                                    &row.kc_ual,
                                    now_ts,
                                    retry_delay_secs,
                                    &error_message,
                                )
                                .await
                            {
                                tracing::warn!(
                                    blockchain_id = %blockchain_id,
                                    paranet = %row.paranet_ual,
                                    kc_ual = %row.kc_ual,
                                    error = %e,
                                    "Failed to update retry state after fetch failure"
                                );
                            }

                            if row.retry_count.saturating_add(1) >= retries_limit {
                                outcome.retry_exhausted = 1;
                            }
                            outcome
                        }
                    }
                }
            })
            .buffer_unordered(max_in_flight);

        let mut synced = 0usize;
        let mut failed = 0usize;
        let mut retry_exhausted = 0usize;
        let mut local_hits = 0usize;
        let mut network_hits = 0usize;

        tokio::pin!(stream);
        while let Some(row) = stream.next().await {
            synced = synced.saturating_add(row.synced);
            failed = failed.saturating_add(row.failed);
            retry_exhausted = retry_exhausted.saturating_add(row.retry_exhausted);
            local_hits = local_hits.saturating_add(row.local_hits);
            network_hits = network_hits.saturating_add(row.network_hits);
        }

        (synced, failed, retry_exhausted, local_hits, network_hits)
    }
}

#[derive(Clone)]
struct ParanetSyncTarget {
    paranet_ual: String,
    paranet_id: String,
    paranet_id_b256: dkg_blockchain::B256,
    access_policy: AccessPolicy,
}

#[derive(Default)]
struct RowOutcome {
    synced: usize,
    failed: usize,
    retry_exhausted: usize,
    local_hits: usize,
    network_hits: usize,
}
