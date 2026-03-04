use std::sync::Arc;

use dkg_blockchain::{BlockchainId, ContractName};
use dkg_observability as observability;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    DkgSyncConfig,
    discovery::{self, DiscoveryWorker},
    pipeline::{DkgSyncPipeline, types::QueueOutcome},
    queue,
};
use crate::tasks::periodic::DkgSyncDeps;

pub(crate) struct DkgSyncTask {
    config: DkgSyncConfig,
    reorg_buffer_blocks: u64,
    deps: DkgSyncDeps,
    notify: Arc<Notify>,
}

impl DkgSyncTask {
    pub(crate) fn new(deps: DkgSyncDeps, config: DkgSyncConfig, reorg_buffer_blocks: u64) -> Self {
        Self {
            config,
            reorg_buffer_blocks,
            deps,
            notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) async fn run(self, blockchain_id: &BlockchainId, shutdown: CancellationToken) {
        let queue_cfg = &self.config.queue_processor;
        tracing::info!(
            blockchain_id = %blockchain_id,
            inflight_kc_limit = queue_cfg.inflight_kc_limit,
            dispatch_max_kc_per_attempt = queue_cfg.dispatch_max_kc_per_attempt,
            stage_channel_message_buffer = queue_cfg.stage_channel_message_buffer,
            filter_max_kc_per_chunk = queue_cfg.filter_max_kc_per_chunk,
            fetch_max_kc_per_batch = queue_cfg.fetch_max_kc_per_batch,
            fetch_peer_fanout_concurrency = queue_cfg.fetch_peer_fanout_concurrency,
            fetch_max_ka_per_batch = queue_cfg.fetch_max_ka_per_batch,
            insert_kc_concurrency = queue_cfg.insert_kc_concurrency,
            "Starting DKG sync queue processor"
        );

        observability::record_sync_pipeline_inflight(0);

        let sync_pipeline = DkgSyncPipeline::new(self.deps.clone(), self.config.clone());
        let (outcome_tx, outcome_rx) = mpsc::channel::<Vec<QueueOutcome>>(
            self.config
                .queue_processor
                .stage_channel_message_buffer
                .max(1)
                * 2,
        );
        let pipeline_runtime = sync_pipeline.start(blockchain_id, outcome_tx);

        let discovery_handle = self.start_discovery(blockchain_id, shutdown.clone()).await;

        queue::queue_loop::run(
            self.deps.clone(),
            self.config.clone(),
            Arc::clone(&self.notify),
            blockchain_id.clone(),
            pipeline_runtime.input_tx.clone(),
            outcome_rx,
            shutdown,
        )
        .await;

        pipeline_runtime.shutdown().await;

        if let Some(discovery_handle) = discovery_handle
            && let Err(error) = discovery_handle.await
        {
            tracing::error!(
                blockchain_id = %blockchain_id,
                error = ?error,
                "DKG sync discovery panicked"
            );
        }

        observability::record_sync_pipeline_inflight(0);
        tracing::info!(blockchain_id = %blockchain_id, "DKG sync queue processor stopped");
    }

    async fn start_discovery(
        &self,
        blockchain_id: &BlockchainId,
        shutdown: CancellationToken,
    ) -> Option<tokio::task::JoinHandle<()>> {
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
                    "Failed to get KC storage contract addresses for DKG sync discovery"
                );
                return None;
            }
        };

        if contract_addresses.is_empty() {
            tracing::info!(
                blockchain_id = %blockchain_id,
                "No KC storage contracts found for DKG sync discovery"
            );
            return None;
        }

        tracing::info!(
            blockchain_id = %blockchain_id,
            contracts = contract_addresses.len(),
            reorg_buffer_blocks = self.reorg_buffer_blocks,
            "Starting DKG sync discovery"
        );

        Some(tokio::spawn(discovery::discovery_loop::run(
            DiscoveryWorker::new(self.deps.clone(), self.config.clone()),
            self.deps.clone(),
            self.config.clone(),
            self.reorg_buffer_blocks,
            blockchain_id.clone(),
            contract_addresses,
            Arc::clone(&self.notify),
            shutdown,
        )))
    }
}
