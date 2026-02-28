//! Long-lived data pipeline for sync backfill.

use tokio::{sync::mpsc, task::JoinHandle};

use dkg_blockchain::BlockchainId;

use super::{
    SyncConfig,
    fetch::run_fetch_stage,
    filter::run_filter_stage,
    insert::run_insert_stage,
    types::{FetchedKc, KcToSync, QueueKcWorkItem, QueueOutcome},
};
use crate::tasks::periodic::SyncDeps;

// Keep peer fanout fixed to avoid overloading the network during backfill.
const FETCH_FANOUT_CONCURRENCY: usize = 3;

pub(crate) struct SyncPipeline {
    config: SyncConfig,
    deps: SyncDeps,
}

pub(crate) struct SyncPipelineRuntime {
    pub(crate) input_tx: mpsc::Sender<Vec<QueueKcWorkItem>>,
    filter_handle: JoinHandle<()>,
    fetch_handle: JoinHandle<()>,
    insert_handle: JoinHandle<()>,
}

impl SyncPipelineRuntime {
    pub(crate) async fn shutdown(self) {
        drop(self.input_tx);

        for (name, handle) in [
            ("filter", self.filter_handle),
            ("fetch", self.fetch_handle),
            ("insert", self.insert_handle),
        ] {
            if let Err(error) = handle.await {
                tracing::error!(stage = name, error = ?error, "Sync pipeline stage panicked");
            }
        }
    }
}

impl SyncPipeline {
    pub(crate) fn new(deps: SyncDeps, config: SyncConfig) -> Self {
        Self { config, deps }
    }

    pub(crate) fn enough_peers_for_fetch(&self, blockchain_id: &BlockchainId) -> bool {
        let total_shard_peers = self.deps.peer_registry.shard_peer_count(blockchain_id);
        let identified_peers = self
            .deps
            .peer_registry
            .identified_shard_peer_count(blockchain_id);
        let min_required = (total_shard_peers / 3).max(3);
        identified_peers >= min_required
    }

    pub(crate) fn start(
        &self,
        blockchain_id: &BlockchainId,
        current_epoch: Option<u64>,
        queue_outcome_tx: mpsc::Sender<Vec<QueueOutcome>>,
    ) -> SyncPipelineRuntime {
        let pipeline_capacity = self.config.pipeline_capacity.max(1);
        let stage_channel_buffer = self.config.stage_channel_buffer.max(1);
        let filter_batch_size = self.config.filter_batch_size.max(1);
        let insert_batch_concurrency = self.config.insert_batch_concurrency.max(1);
        let max_assets_per_fetch_batch = self.config.max_assets_per_fetch_batch.max(1);

        let (input_tx, input_rx) = mpsc::channel::<Vec<QueueKcWorkItem>>(stage_channel_buffer);
        let (filter_tx, filter_rx) = mpsc::channel::<Vec<KcToSync>>(stage_channel_buffer);
        let (fetch_tx, fetch_rx) = mpsc::channel::<Vec<FetchedKc>>(stage_channel_buffer);

        let filter_handle = {
            let blockchain_id = blockchain_id.clone();
            let kc_chain_metadata_repository = self.deps.kc_chain_metadata_repository.clone();
            let triple_store_assertions = self.deps.triple_store_assertions.clone();
            let outcome_tx = queue_outcome_tx.clone();
            tokio::spawn(async move {
                run_filter_stage(
                    input_rx,
                    filter_batch_size,
                    blockchain_id,
                    current_epoch,
                    kc_chain_metadata_repository,
                    triple_store_assertions,
                    filter_tx,
                    outcome_tx,
                )
                .await;
            })
        };

        let fetch_handle = {
            let blockchain_id = blockchain_id.clone();
            let network_manager = self.deps.network_manager.clone();
            let assertion_validation = self.deps.assertion_validation.clone();
            let peer_registry = self.deps.peer_registry.clone();
            let outcome_tx = queue_outcome_tx.clone();
            tokio::spawn(async move {
                run_fetch_stage(
                    filter_rx,
                    pipeline_capacity,
                    FETCH_FANOUT_CONCURRENCY,
                    max_assets_per_fetch_batch,
                    blockchain_id,
                    network_manager,
                    assertion_validation,
                    peer_registry,
                    fetch_tx,
                    outcome_tx,
                )
                .await;
            })
        };

        let insert_handle = {
            let blockchain_id = blockchain_id.clone();
            let triple_store_assertions = self.deps.triple_store_assertions.clone();
            tokio::spawn(async move {
                run_insert_stage(
                    fetch_rx,
                    blockchain_id,
                    insert_batch_concurrency,
                    triple_store_assertions,
                    queue_outcome_tx,
                )
                .await;
            })
        };

        SyncPipelineRuntime {
            input_tx,
            filter_handle,
            fetch_handle,
            insert_handle,
        }
    }
}
